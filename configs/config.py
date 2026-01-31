import os

ENV = os.getenv("ENV", "dev")

CONFIG = {
    "dev": {
        "storage_account": "YOUR_STORAGE_ACCOUNT_NAME",
        "containers": {
            "source": "source",
            "bronze": "bronze",
            "silver": "silver",
            "gold": "gold"
        },
        "checkpoint_base": "checkpoints"
    },
    "prod": {
        "storage_account": "YOUR_STORAGE_ACCOUNT_NAME",
        "containers": {
            "source": "source",
            "bronze": "bronze",
            "silver": "silver",
            "gold": "gold"
        },
        "checkpoint_base": "checkpoints"
    }
}

cfg = CONFIG[ENV]
