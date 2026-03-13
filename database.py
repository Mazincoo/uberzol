import json
import asyncio
from pathlib import Path
from typing import Dict, Any

DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

class JSONDatabase:

    def __init__(self, filename: str):
        self.path = DATA_DIR / filename
        self.data: Dict[str, Any] = {}
        self.lock = asyncio.Lock()
        self._load()

    # =========================
    # LOAD
    # =========================
    def _load(self):
        if not self.path.exists():
            self.data = {}
            self._save()
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except Exception:
            self.data = {}

    # =========================
    # SAVE
    # =========================
    def _save(self):
        tmp = self.path.with_suffix(".tmp")

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

        tmp.replace(self.path)

    async def save(self):
        async with self.lock:
            self._save()

    # =========================
    # GET
    # =========================
    def get(self, key: str, default=None):
        return self.data.get(key, default)

    # =========================
    # SET
    # =========================
    async def set(self, key: str, value):
        async with self.lock:
            self.data[key] = value
            self._save()

    # =========================
    # DELETE
    # =========================
    async def delete(self, key: str):
        async with self.lock:
            if key in self.data:
                del self.data[key]
                self._save()

    # =========================
    # ALL
    # =========================
    def all(self):
        return self.data

    # =========================
    # EXISTS
    # =========================
    def exists(self, key):
        return key in self.data


# =========================
# DATABASE INSTANCES
# =========================

clients_db = JSONDatabase("clients.json")
drivers_db = JSONDatabase("drivers.json")
wallets_db = JSONDatabase("wallets.json")
trips_db = JSONDatabase("trips.json")
cancellations_db = JSONDatabase("cancellations.json")
online_db = JSONDatabase("online_drivers.json")
app_state_db = JSONDatabase("app_state.json")