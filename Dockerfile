# ========================================
# OpenAI Pool Orchestrator  Docker 镜像
# ========================================
FROM python:3.12-slim

# 禁用缓冲，让 Python 日志立即输出到 docker logs 终端
ENV PYTHONUNBUFFERED=1

# 系统依赖（curl-cffi 编译需要）
# 替换 apt 为清华国内源加速
RUN sed -i 's/deb.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc g++ make curl libssl-dev libffi-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先拷贝依赖清单，设置 pip 国内源加速
COPY requirements.txt pyproject.toml ./
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -e .

# 拷贝项目全部代码
COPY . .

# 再次以可编辑模式安装，确保 static 资源被正确注册
RUN pip install --no-cache-dir -e .

# 数据卷：配置和 Token 持久化
VOLUME ["/app/data", "/app/config"]

# Web UI 端口
EXPOSE 18421

# 启动命令（可在 docker run 时通过追加参数切换模式，如 --cli）
ENTRYPOINT ["python", "run.py"]
