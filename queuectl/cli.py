import click
import sys
import json
from pathlib import Path
from .db import Database
from .config import ConfigManager
from .queue import QueueManager
from .worker_manager import WorkerManager
from .version import __version__


@click.group()
@click.version_option(version=__version__)
@click.help_option('--help', '-h')
def cli():
    """QueueCTL - Production-grade CLI-based background job queue system.
    
    QueueCTL allows you to enqueue shell commands as background jobs,
    process them with multiple concurrent workers, and manage failures
    with automatic retries and a Dead Letter Queue.
    
    Examples:
        queuectl enqueue '{"command": "echo hello"}'
        queuectl worker start --count 2
        queuectl status
        queuectl dlq list
    """
    pass


@cli.command()
@click.argument('job_spec', required=False)
@click.option('--file', type=click.Path(exists=True), help='Read job specification from file')
@click.option('--command', help='Command to execute (alternative to JSON)')
@click.option('--id', 'job_id', help='Job ID (optional, auto-generated if not provided)')
@click.option('--max-retries', type=int, help='Maximum retry attempts')
@click.option('--priority', type=int, default=0, help='Job priority (higher values processed first)')
@click.option('--timeout', type=int, help='Job timeout in seconds')
def enqueue(job_spec, file, command, job_id, max_retries, priority, timeout):
    """Enqueue a new job for processing.
    
    JOB_SPEC should be a JSON string containing job details.
    Use --file to read job specification from a file instead.
    Alternatively, use --command with optional flags for simpler job creation.
    
    Examples:
        queuectl enqueue '{"command": "echo hello world"}'
        queuectl enqueue --file job.json
        queuectl enqueue --command "echo hello" --id "my-job" --max-retries 2
        
    Windows PowerShell examples:
        queuectl enqueue --command "echo hello world"
        queuectl enqueue --command "dir" --id "list-files" --timeout 30
    """
    try:
        input_methods = sum([bool(file), bool(job_spec), bool(command)])
        
        if input_methods == 0:
            click.echo("Error: Must provide job specification via JSON string, --file, or --command", err=True)
            sys.exit(1)
        elif input_methods > 1:
            click.echo("Error: Cannot specify multiple input methods (choose one: JSON string, --file, or --command)", err=True)
            sys.exit(1)
        
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config = config_manager.get_config()
        
        if file:
            with open(file, 'r') as f:
                job_spec_str = f.read()
        elif job_spec:
            job_spec_str = job_spec
        elif command:
            job_dict = {"command": command}
            if job_id:
                job_dict["id"] = job_id
            if max_retries is not None:
                job_dict["max_retries"] = max_retries
            if priority != 0:
                job_dict["priority"] = priority
            if timeout:
                job_dict["timeout_seconds"] = timeout
            
            job_spec_str = json.dumps(job_dict)
        
        queue_manager = QueueManager(Database(config.db_path))
        result_job_id = queue_manager.validate_and_enqueue(job_spec_str, config.max_retries)
        
        click.echo(f"Job enqueued successfully with ID: {result_job_id}")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def worker():
    """Manage worker processes that execute jobs."""
    pass


@worker.command()
@click.option('--count', default=1, help='Number of worker processes to start')
@click.option('--detach', is_flag=True, help='Run workers in background')
@click.option('--poll-interval-ms', default=500, help='Polling interval in milliseconds')
def start(count, detach, poll_interval_ms):
    """Start worker processes to execute jobs.
    
    Examples:
        queuectl worker start --count 3
        queuectl worker start --count 2 --detach
        queuectl worker start --poll-interval-ms 1000
    """
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config = config_manager.get_config()
        
        database = Database(config.db_path)
        recovered_jobs = database.recover_stale_jobs(
            config.stale_worker_timeout_seconds, 
            config.backoff_base
        )
        
        if recovered_jobs > 0:
            click.echo(f"Recovered {recovered_jobs} stale jobs from previous workers")
        
        worker_manager = WorkerManager(config.db_path)
        
        if detach:
            click.echo(f"Starting {count} worker processes in background")
        else:
            click.echo(f"Starting {count} worker processes (Press Ctrl+C to stop)")
        
        worker_manager.start_workers(count, poll_interval_ms, detach)
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@worker.command()
def stop():
    """Stop all running worker processes gracefully.
    
    Examples:
        queuectl worker stop
    """
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config = config_manager.get_config()
        
        worker_manager = WorkerManager(config.db_path)
        worker_manager.stop_workers()
        
        click.echo("All workers stopped successfully")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
def status():
    """Show queue status and worker information.
    
    Examples:
        queuectl status
    """
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config = config_manager.get_config()
        
        database = Database(config.db_path)
        queue_manager = QueueManager(database)
        
        counts = queue_manager.get_job_counts()
        active_workers = database.get_active_workers(config.stale_worker_timeout_seconds)
        recent_failures = queue_manager.get_recent_failures(3)
        
        click.echo("=== QueueCTL Status ===")
        click.echo()
        click.echo("Job Counts:")
        click.echo(f"  Pending:    {counts['pending']}")
        click.echo(f"  Processing: {counts['processing']}")
        click.echo(f"  Completed:  {counts['completed']}")
        click.echo(f"  Failed:     {counts['failed']}")
        click.echo(f"  DLQ:        {counts['dlq']}")
        click.echo()
        click.echo(f"Active Workers: {len(active_workers)}")
        
        if active_workers:
            for worker in active_workers:
                click.echo(f"  {worker.worker_id} (PID: {worker.pid}, Host: {worker.hostname})")
        
        if recent_failures:
            click.echo()
            click.echo("Recent Failures:")
            for job in recent_failures:
                error_preview = job.last_error[:50] + "..." if job.last_error and len(job.last_error) > 50 else job.last_error
                click.echo(f"  {job.id}: {error_preview}")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--state', help='Filter by job state (pending, processing, completed, failed)')
@click.option('--limit', default=10, help='Maximum number of jobs to show')
@click.option('--since', help='Show jobs since ISO datetime (e.g., 2023-01-01T00:00:00)')
@click.option('--sort', default='created_at', help='Sort by field (created_at, updated_at, priority)')
def list(state, limit, since, sort):
    """List jobs with optional filtering and sorting.
    
    Examples:
        queuectl list
        queuectl list --state pending --limit 20
        queuectl list --since 2023-01-01T00:00:00 --sort priority
    """
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config = config_manager.get_config()
        
        queue_manager = QueueManager(Database(config.db_path))
        jobs = queue_manager.list_jobs(state, limit, since, sort)
        
        if not jobs:
            click.echo("No jobs found")
            return
        
        click.echo(f"{'ID':<20} {'State':<12} {'Command':<30} {'Attempts':<8} {'Created':<20}")
        click.echo("-" * 90)
        
        for job in jobs:
            command_preview = job.command[:27] + "..." if len(job.command) > 30 else job.command
            created_str = job.created_at.strftime("%Y-%m-%d %H:%M:%S")
            click.echo(f"{job.id:<20} {job.state.value:<12} {command_preview:<30} {job.attempts:<8} {created_str:<20}")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def dlq():
    """Manage Dead Letter Queue (failed jobs that exceeded retry limits)."""
    pass


@dlq.command()
@click.option('--limit', default=10, help='Maximum number of jobs to show')
def list(limit):
    """List jobs in the Dead Letter Queue.
    
    Examples:
        queuectl dlq list
        queuectl dlq list --limit 20
    """
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config = config_manager.get_config()
        
        queue_manager = QueueManager(Database(config.db_path))
        dlq_jobs = queue_manager.list_dlq(limit)
        
        if not dlq_jobs:
            click.echo("No jobs in Dead Letter Queue")
            return
        
        click.echo(f"{'DLQ ID':<20} {'Original ID':<20} {'Command':<30} {'Attempts':<8} {'Moved At':<20}")
        click.echo("-" * 98)
        
        for job in dlq_jobs:
            command_preview = job.command[:27] + "..." if len(job.command) > 30 else job.command
            moved_str = job.moved_at.strftime("%Y-%m-%d %H:%M:%S")
            click.echo(f"{job.id:<20} {job.original_job_id:<20} {command_preview:<30} {job.attempts:<8} {moved_str:<20}")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@dlq.command()
@click.argument('job_id')
@click.option('--same-id', is_flag=True, help='Keep the same job ID when retrying (risky)')
def retry(job_id, same_id):
    """Retry a job from the Dead Letter Queue.
    
    By default, creates a new job with a new ID. Use --same-id to keep
    the original job ID (may fail if a job with that ID already exists).
    
    Examples:
        queuectl dlq retry abc123
        queuectl dlq retry abc123 --same-id
    """
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config = config_manager.get_config()
        
        queue_manager = QueueManager(Database(config.db_path))
        new_job_id = queue_manager.retry_from_dlq(job_id, same_id)
        
        click.echo(f"Job retried successfully with ID: {new_job_id}")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@dlq.command()
@click.option('--older-than', type=int, help='Purge jobs older than N days')
@click.option('--force', is_flag=True, help='Confirm purge operation')
def purge(older_than, force):
    """Purge jobs from the Dead Letter Queue.
    
    WARNING: This permanently deletes jobs from the DLQ.
    Use --force to confirm the operation.
    
    Examples:
        queuectl dlq purge --older-than 30 --force
        queuectl dlq purge --force  # Purges all DLQ jobs
    """
    try:
        if not force:
            click.echo("Error: Purge operation requires --force flag for confirmation", err=True)
            sys.exit(1)
        
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config = config_manager.get_config()
        
        queue_manager = QueueManager(Database(config.db_path))
        
        if older_than:
            click.echo(f"Purging DLQ jobs older than {older_than} days...")
        else:
            click.echo("Purging all DLQ jobs...")
        
        queue_manager.purge_dlq(older_than)
        click.echo("DLQ purge completed successfully")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def config():
    pass


@config.command()
@click.argument('key')
@click.argument('value')
def set(key, value):
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config_manager.set(key, value)
        click.echo(f"Set {key} = {value}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@config.command()
@click.argument('key')
def get(key):
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        value = config_manager.get(key)
        if value is not None:
            click.echo(value)
        else:
            click.echo(f"Configuration key '{key}' not found", err=True)
            sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@config.command()
def list():
    try:
        config_manager = ConfigManager(Database('.data/queuectl.db'))
        config_dict = config_manager.list_all()
        for key, value in sorted(config_dict.items()):
            click.echo(f"{key} = {value}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()