"""CLI plugin; runtime imports are deferred until a command is invoked."""

import json

import click


@click.group()
def ingestion():
    """机器人本地数据回流到 OBS。"""


@ingestion.command("start")
@click.option(
    "--config", "config_path", required=True, type=click.Path(exists=True, dir_okay=False)
)
def start(config_path):
    """启动后台目录监控。"""
    from . import lifecycle

    try:
        result = lifecycle.start(config_path)
    except (OSError, ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"回流进程已启动，PID={result['pid']}，工作目录={result['work_dir']}")


@ingestion.command("status")
@click.option("--json", "as_json", is_flag=True, help="输出 JSON 状态快照")
def status(as_json):
    """离线查询进程及最近的回流状态。"""
    from . import lifecycle

    result = lifecycle.status()
    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False))
        return
    click.echo(f"进程: {'运行中' if result['running'] else '已停止'}")
    click.echo(f"批次统计: {result.get('counts', {})}")
    click.echo(f"当前批次: {result.get('active_batch') or '-'}")
    click.echo(f"上传进度: {result.get('progress', {})}")
    click.echo(f"上报状态: {result.get('reporter', {})}")
    for item in result.get("batch_errors", []):
        click.echo(f"{item['path']}: {item['error']}")
    if result.get("error"):
        click.echo(f"最近错误: {result['error']}")


@ingestion.command("stop")
def stop():
    """尽快结束当前上传并退出后台进程。"""
    from . import lifecycle

    try:
        lifecycle.stop()
    except (OSError, ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo("回流进程已停止")
