#!/usr/bin/env python3
"""无 Docker 环境下用 pgserver 常驻一个 PostgreSQL，并把 DSN 写入文件。

用法: python3 scripts/pgserver_hold.py <数据目录> <DSN输出文件> [停止文件]
"""
import os
import pathlib
import sys
import time

import pgserver


def main():
    data_dir, dsn_file = sys.argv[1], sys.argv[2]
    stop_file = sys.argv[3] if len(sys.argv) > 3 else None
    srv = pgserver.get_server(data_dir)
    uri = srv.get_uri()
    pathlib.Path(dsn_file).write_text(uri)
    print(f"PostgreSQL 已启动: {uri}", flush=True)
    while True:
        if stop_file and os.path.exists(stop_file):
            return  # postgres 子进程随会话结束回收
        time.sleep(1)


if __name__ == "__main__":
    main()
