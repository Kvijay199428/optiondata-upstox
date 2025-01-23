import subprocess
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.panel import Panel
from rich.layout import Layout
from rich.columns import Columns
from rich import box
from rich.live import Live
from rich.text import Text
from rich.table import Table

console = Console()

def create_progress():
    """Create a compact progress bar with spinner."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        expand=True
    )

def run_command(command):
    """Run a shell command and return the output and status."""
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.stdout.strip(), result.stderr.strip(), result.returncode

def check_for_changes():
    """Check if there are any changes to commit."""
    stdout, stderr, returncode = run_command("git status --porcelain")
    return bool(stdout)

def get_repo_stats():
    """Get repository statistics."""
    stats = {}
    stdout, _, _ = run_command("git branch --show-current")
    stats['branch'] = stdout or "unknown"
    
    stdout, _, _ = run_command("git log -1 --format='%h|%s|%ar'")
    if stdout and '|' in stdout:
        hash_id, message, time_ago = stdout.split('|')
        stats['last_commit'] = {
            'hash': hash_id,
            'message': message[:30] + ('...' if len(message) > 30 else ''),
            'time': time_ago
        }
    else:
        stats['last_commit'] = None
    
    stdout, _, _ = run_command("git status --porcelain")
    stats['changed_files'] = []
    for line in stdout.split('\n'):
        if line.strip():
            status = line[:2]
            filename = line[3:].strip()
            stats['changed_files'].append((status, filename))
    
    stats['modified_files'] = len(stats['changed_files'])
    return stats

class DashboardManager:
    def __init__(self):
        self.progress = create_progress()
        self.status_message = ""
        self.operation_status = "idle"
        self.pushed_files = []
        self.initial_file_statuses = {}

    def store_initial_statuses(self):
        """Store the initial status of files before operations."""
        stdout, _, _ = run_command("git status --porcelain")
        for line in stdout.split('\n'):
            if line.strip():
                status = line[:2]
                filename = line[3:].strip()
                self.initial_file_statuses[filename] = status

    def create_info_section(self, stats):
        """Create a compact info section combining branch and modified files."""
        info_text = Text()
        info_text.append("Branch: ", style="dim")
        info_text.append(f"🔖 {stats['branch']}", style="bold cyan")
        info_text.append(" | ", style="dim")
        info_text.append("Files: ", style="dim")
        info_text.append(f"📝 {stats['modified_files']}", style="bold magenta")
        return Panel(info_text, box=box.ROUNDED)

    def create_files_table(self, files_data, title):
        """Create a table for displaying files."""
        table = Table(
            show_header=True,
            header_style="bold blue",
            box=box.ROUNDED,
            padding=(0, 1),
            show_edge=False
        )
        table.add_column("Status", style="cyan", width=8)
        table.add_column("File", style="white")

        status_map = {
            'M ': "Modified",
            'A ': "Added",
            'D ': "Deleted",
            'R ': "Renamed",
            'C ': "Copied",
            '??': "Untracked"
        }

        for status, filename in files_data:
            status_text = status_map.get(status, status)
            table.add_row(status_text, filename)

        return Panel(table, title=title, box=box.ROUNDED)

    def create_dashboard(self, stats):
        """Create a compact dashboard layout."""
        layout = Layout()
        
        if self.operation_status == "success" and self.pushed_files:
            # Show both changed and pushed files after successful push
            files_layout = Layout()
            initial_files = [(self.initial_file_statuses.get(f, '??'), f) 
                           for f in self.pushed_files]
            
            # Split the files section into two columns
            files_layout.split_row(
                Layout(self.create_files_table(
                    initial_files, 
                    "📤 Pushed Files",
                    
                ))
            )
            
            layout.split_column(
                Layout(name="info", size=3),
                Layout(name="files", size=8),
                Layout(name="progress", size=3)
            )
            layout["files"].update(files_layout)
        else:
            # Show regular dashboard during operation
            layout.split_column(
                Layout(name="info", size=3),
                Layout(name="files", size=8),
                Layout(name="progress", size=3)
            )
            layout["files"].update(self.create_files_table(
                stats['changed_files'], 
                "📋 Changed Files"
            ))

        layout["info"].update(self.create_info_section(stats))
        layout["progress"].update(self.create_progress_section())
        
        return Panel(
            layout,
            title="[bold blue]Git Operations[/bold blue]",
            subtitle=f"[dim]{datetime.now().strftime('%H:%M:%S')}[/dim]",
            box=box.ROUNDED
        )

    def create_progress_section(self):
        """Create a compact progress section."""
        if self.operation_status == "idle":
            content = Text("Ready for operations", style="dim")
        elif self.operation_status == "running":
            content = self.progress
        else:
            content = Text(
                self.status_message,
                style="bold green" if self.operation_status == "success" else "bold red"
            )
        return Panel(content, box=box.ROUNDED)

    def git_operations(self, live):
        """Perform git operations with live updates."""
        if not check_for_changes():
            self.status_message = "No changes to commit"
            self.operation_status = "success"
            return False

        self.operation_status = "running"
        try:
            # Store initial file statuses
            self.store_initial_statuses()
            self.pushed_files = list(self.initial_file_statuses.keys())

            task1 = self.progress.add_task("Adding changes", total=100)
            stdout, stderr, returncode = run_command("git add .")
            if returncode != 0:
                raise Exception(f"Git add failed: {stderr}")
            self.progress.update(task1, completed=100)
            
            task2 = self.progress.add_task("Committing changes", total=100)
            stdout, stderr, returncode = run_command(
                'git commit -m "Automated commit of updated scripts"'
            )
            if returncode != 0:
                raise Exception(f"Git commit failed: {stderr}")
            self.progress.update(task2, completed=100)
            
            task3 = self.progress.add_task("Pushing to remote", total=100)
            stdout, stderr, returncode = run_command("git push origin main")
            if returncode != 0:
                raise Exception(f"Git push failed: {stderr}")
            self.progress.update(task3, completed=100)
            
            num_files = len(self.pushed_files)
            self.status_message = f"✓ Successfully pushed {num_files} file{'s' if num_files != 1 else ''}"
            self.operation_status = "success"
            return True
            
        except Exception as e:
            self.status_message = f"✗ Error: {str(e)}"
            self.operation_status = "error"
            return False

if __name__ == "__main__":
    dashboard_manager = DashboardManager()
    console.clear()
    
    with Live(auto_refresh=False) as live:
        # Initial dashboard
        stats = get_repo_stats()
        live.update(dashboard_manager.create_dashboard(stats))
        live.refresh()
        
        # Perform git operations
        dashboard_manager.git_operations(live)
        
        # Final update with results
        stats = get_repo_stats()
        live.update(dashboard_manager.create_dashboard(stats))
        live.refresh()
        
        # Keep the dashboard visible
        console.input("\nPress Enter to exit...")
