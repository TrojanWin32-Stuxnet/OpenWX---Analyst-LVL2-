import logging
import uvicorn
from server.config import load_config

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = load_config()
    server_cfg = config.get("server", {})
    uvicorn.run(
        "server.app:app",
        host=server_cfg.get("host", "0.0.0.0"),
        port=server_cfg.get("port", 8080),
        reload=True,
        reload_dirs=["server", "config"],
        reload_excludes=["venv", "cache", "*.pyc", "__pycache__"],
    )

if __name__ == "__main__":
    main()
