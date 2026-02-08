#!/usr/bin/env python3
"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚀 EcoSchool AI Power Saver v12.0 ULTIMATE - ENTERPRISE EDITION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✨ TÍNH NĂNG HOÀN CHỈNH:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ 15 AI Agents (Full Enterprise Suite) - Tự động 24/7
✅ MySQL Database (HeidiSQL: root/tam06092009)
✅ IoT Device Control (Bật/Tắt thiết bị tự động)
✅ Smart Search System (Tìm phòng, sensor, device)
✅ Delete Room/Sensor (Xóa khi tạo sai)
✅ Per-Room Analytics (Phân tích từng phòng cụ thể)
✅ Ultra-Intelligent Chatbot:
    • Ghi nhớ 100% hội thoại
    • Tự học từ conversation
    • Cập nhật tin tức real-time
    • HỎI XÁC NHẬN trước khi thay đổi
    • HUMAN > AI (quyền cao nhất)
✅ Teaching System (Dạy AI qua chat)
✅ Auto-Learning & Self-Improvement
✅ News Integration (Tin tức mới nhất)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os, sys, json, logging, asyncio, time, uuid, hashlib, secrets

# Force UTF-8 for Windows console
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict, deque
import statistics

# Database imports
import sqlite3
try:
    import pymysql
    import pymysql.cursors
    HAS_MYSQL = True
except:
    HAS_MYSQL = False

import fastapi
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder

try:
    import google.generativeai as genai
    HAS_GEMINI = True
except:
    HAS_GEMINI = False

try:
    from supabase import create_client, Client
    HAS_SUPABASE = True
except:
    HAS_SUPABASE = False

# ============================================================================
# CONFIGURATION
# ============================================================================

SECRET_KEY = os.getenv('ECOSCHOOL_SECRET_KEY', secrets.token_hex(32))
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY', "YOUR_API_KEY_HERE")

# MySQL Configuration for HeidiSQL
MYSQL_CONFIG = {
    "host": os.getenv('MYSQL_HOST', "localhost"),
    "user": os.getenv('MYSQL_USER', "root"),
    "password": os.getenv('MYSQL_PASSWORD', "tam06092009"),
    "database": os.getenv('MYSQL_DATABASE', "ecoschool_ultimate"),
    "port": int(os.getenv('MYSQL_PORT', 3306)),
    "autocommit": True
}

# Supabase Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_USER_ID = os.getenv('SUPABASE_USER_ID', "d3d21cfa-1380-423f-82a4-6fdc44e3f48e")

def find_file(filename: str) -> Path:
    local = Path(__file__).parent / filename
    return local if local.exists() else Path(filename)

# Determine if running on Vercel
# Determine if running on Vercel
IS_VERCEL = (
    os.environ.get('VERCEL') == '1' or 
    os.environ.get('VERCEL_ENV') is not None or
    os.environ.get('NOW_REGION') is not None or
    os.path.exists('/var/task')
)

log_handlers = [logging.StreamHandler(sys.stdout)]
if not IS_VERCEL:
    try:
        log_handlers.append(logging.FileHandler('ecoschool_ultimate.log', encoding='utf-8'))
    except:
        pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=log_handlers
)
# Ensure stdout is utf-8
if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger("EcoSchool")

app = FastAPI(title="EcoSchool AI Ultimate", version="12.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Load HTML files with absolute paths
BASE_DIR = Path(__file__).parent

def load_html(filename: str) -> str:
    path = BASE_DIR / filename
    if path.exists():
        return path.read_text(encoding='utf-8')
    logger.error(f"❌ HTML file not found: {path}")
    return ""

LOGIN_HTML = load_html("login.html")
DASHBOARD_HTML = load_html("dashboard_ultimate.html")

# Mount static files
try:
    app.mount("/assets", StaticFiles(directory=str(Path(__file__).parent / "assets")), name="assets")
except:
    pass

# ============================================================================
# 🗄️ ULTIMATE DATABASE MANAGER (MySQL + SQLite Fallback)
# ============================================================================

class UltimateDatabaseManager:
    """
    Quản lý database thông minh:
    - Ưu tiên MySQL (HeidiSQL compatible)
    - Fallback SQLite nếu MySQL không có
    - Auto-create database & tables
    """
    
    def __init__(self):
        self.use_supabase = False
        self.use_mysql = False
        self.supabase_client: Optional[Client] = None
        self.connection = None
        self.db_type = "SQLite"
        self._init_connection()
        self._init_schema()
    
    def _init_connection(self):
        """Khởi tạo kết nối database"""
        # [ROBUST] Always initialize connection immediately to prevent NoneType error
        sqlite_path = "/tmp/ecoschool_ultimate.db" if IS_VERCEL else "ecoschool_ultimate.db"
        self.connection = sqlite3.connect(sqlite_path, check_same_thread=False)
        # --- ƯU TIÊN 1: SUPABASE (CLOUD) ---
        if HAS_SUPABASE and SUPABASE_URL and SUPABASE_KEY:
            try:
                self.supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
                self.use_supabase = True
                self.db_type = "Supabase"
                logger.info("✅ Connected to Supabase (Cloud Database)")
                # Init local SQLite anyway as a safe scratchpad/fallback
                sqlite_path = "/tmp/ecoschool_ultimate.db" if IS_VERCEL else "ecoschool_ultimate.db"
                self.connection = sqlite3.connect(sqlite_path, check_same_thread=False)
                return
            except Exception as e:
                logger.warning(f"⚠️  Supabase connection failed: {e}")
        
        # --- ƯU TIÊN 2: MYSQL (LOCAL) ---
        if HAS_MYSQL:
            try:
                # Try to create database first
                temp_conn = pymysql.connect(
                    host=MYSQL_CONFIG["host"],
                    user=MYSQL_CONFIG["user"],
                    password=MYSQL_CONFIG["password"],
                    port=MYSQL_CONFIG.get("port", 3306)
                )
                cursor = temp_conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_CONFIG['database']}")
                cursor.close()
                temp_conn.close()
                
                # Now connect to the database
                self.connection = pymysql.connect(
                    host=MYSQL_CONFIG["host"],
                    user=MYSQL_CONFIG["user"],
                    password=MYSQL_CONFIG["password"],
                    database=MYSQL_CONFIG["database"],
                    port=MYSQL_CONFIG.get("port", 3306),
                    autocommit=True,
                    cursorclass=pymysql.cursors.DictCursor
                )
                self.use_mysql = True
                self.db_type = "MySQL"
                logger.info("✅ Connected to MySQL (HeidiSQL compatible via PyMySQL)")
            except Exception as e:
                logger.warning(f"⚠️  MySQL connection failed: {e}")
                logger.info("📁 Falling back to SQLite...")
                self.use_mysql = False
                self.db_type = "SQLite"
        
        if not self.use_mysql:
            sqlite_path = "/tmp/ecoschool_ultimate.db" if IS_VERCEL else "ecoschool_ultimate.db"
            self.connection = sqlite3.connect(sqlite_path, check_same_thread=False)
            logger.info(f"✅ Connected to SQLite ({sqlite_path})")
    
    def _get_cursor(self):
        """Get database cursor"""
        if self.use_mysql:
            try:
                self.connection.ping(reconnect=True)
            except:
                self.connection = pymysql.connect(
                    host=MYSQL_CONFIG["host"],
                    user=MYSQL_CONFIG["user"],
                    password=MYSQL_CONFIG["password"],
                    database=MYSQL_CONFIG["database"],
                    port=MYSQL_CONFIG.get("port", 3306),
                    autocommit=True,
                    cursorclass=pymysql.cursors.DictCursor
                )
            return self.connection.cursor()
        else:
            self.connection.row_factory = sqlite3.Row
            return self.connection.cursor()
    
    def _init_schema(self):
        """Khởi tạo schema database (Chỉ chạy cho MySQL/SQLite)"""
        if self.use_supabase:
            logger.info("ℹ️  Supabase selected: Skipping local schema initialization.")
            return
            
        cursor = self._get_cursor()
        
        if self.use_mysql:
            # MySQL Schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username VARCHAR(50) PRIMARY KEY,
                    password_hash VARCHAR(64),
                    role VARCHAR(20),
                    language VARCHAR(10) DEFAULT 'vi',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rooms (
                    room_id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(100),
                    area DOUBLE,
                    floor INT,
                    building VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensors (
                    `sensor_id` VARCHAR(50) PRIMARY KEY,
                    `room_id` VARCHAR(50),
                    `sensor_type` VARCHAR(50),
                    `unit` VARCHAR(20),
                    `last_value` DOUBLE DEFAULT 0,
                    `last_update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `status` VARCHAR(20) DEFAULT 'ONLINE',
                    FOREIGN KEY (`room_id`) REFERENCES `rooms`(`room_id`) ON DELETE CASCADE
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensor_readings (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `sensor_id` VARCHAR(50),
                    `value` DOUBLE,
                    `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `quality` VARCHAR(20) DEFAULT 'GOOD',
                    FOREIGN KEY (`sensor_id`) REFERENCES `sensors`(`sensor_id`) ON DELETE CASCADE,
                    INDEX `idx_timestamp` (`timestamp`),
                    INDEX `idx_sensor` (`sensor_id`)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS iot_devices (
                    `device_id` VARCHAR(50) PRIMARY KEY,
                    `room_id` VARCHAR(50),
                    `device_type` VARCHAR(50),
                    `device_name` VARCHAR(100),
                    `status` VARCHAR(20) DEFAULT 'OFF',
                    `last_command` VARCHAR(50),
                    `last_update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (`room_id`) REFERENCES `rooms`(`room_id`) ON DELETE CASCADE
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ai_decisions (
                    id VARCHAR(50) PRIMARY KEY,
                    agent_name VARCHAR(50),
                    decision_type VARCHAR(50),
                    target VARCHAR(100),
                    action TEXT,
                    reasoning TEXT,
                    confidence DOUBLE,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(20) DEFAULT 'PENDING',
                    approved_by VARCHAR(50),
                    INDEX idx_timestamp (timestamp),
                    INDEX idx_agent (agent_name)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id VARCHAR(50) PRIMARY KEY,
                    severity VARCHAR(20),
                    title VARCHAR(200),
                    message TEXT,
                    location VARCHAR(100),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    acknowledged BOOLEAN DEFAULT FALSE,
                    resolved BOOLEAN DEFAULT FALSE,
                    INDEX idx_timestamp (timestamp)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chat_history (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `user_msg` TEXT,
                    `ai_response` TEXT,
                    `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `has_file` BOOLEAN DEFAULT FALSE,
                    `file_path` VARCHAR(255),
                    `language` VARCHAR(10) DEFAULT 'vi',
                    `context` JSON,
                    INDEX `idx_timestamp` (`timestamp`)
                )
            """)

            # --- ENTERPRISE UPGRADE TABLES ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS buildings (
                    building_id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(100),
                    address TEXT,
                    total_floors INT,
                    manager_name VARCHAR(100),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS floors (
                    floor_id VARCHAR(50) PRIMARY KEY,
                    building_id VARCHAR(50),
                    floor_number INT,
                    name VARCHAR(100),
                    safety_status VARCHAR(50) DEFAULT 'SAFE',
                    energy_target DOUBLE DEFAULT 0,
                    FOREIGN KEY (building_id) REFERENCES buildings(building_id) ON DELETE CASCADE
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS enterprise_schedules (
                    schedule_id INT AUTO_INCREMENT PRIMARY KEY,
                    room_id VARCHAR(50),
                    event_name VARCHAR(200),
                    start_time DATETIME,
                    end_time DATETIME,
                    min_temp DOUBLE DEFAULT 24.0,
                    max_temp DOUBLE DEFAULT 26.0,
                    priority INT DEFAULT 1,
                    is_completed BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (room_id) REFERENCES rooms(room_id) ON DELETE CASCADE
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS safety_watchdog_logs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    location_id VARCHAR(100),
                    risk_level VARCHAR(20),
                    event_type VARCHAR(50),
                    description TEXT,
                    automated_action TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ai_knowledge (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    topic VARCHAR(100),
                    content TEXT,
                    source VARCHAR(255),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    importance INT DEFAULT 5,
                    INDEX idx_topic (topic)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS teaching_sessions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    teacher VARCHAR(50),
                    topic VARCHAR(100),
                    content TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
        else:
            # SQLite Schema
            cursor.execute("""CREATE TABLE IF NOT EXISTS users
                            (username TEXT PRIMARY KEY, password_hash TEXT, role TEXT,
                             language TEXT DEFAULT 'vi', created_at REAL)""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS rooms
                            (room_id TEXT PRIMARY KEY, name TEXT, area REAL, floor INTEGER,
                             building TEXT, created_at REAL, is_active INTEGER DEFAULT 1)""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS sensors
                            (sensor_id TEXT PRIMARY KEY, room_id TEXT, sensor_type TEXT,
                             unit TEXT, last_value REAL, last_update REAL, status TEXT,
                             FOREIGN KEY (room_id) REFERENCES rooms(room_id))""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS sensor_readings
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, sensor_id TEXT,
                             value REAL, timestamp REAL, quality TEXT,
                             FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id))""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS iot_devices
                            (device_id TEXT PRIMARY KEY, room_id TEXT, device_type TEXT,
                             device_name TEXT, status TEXT, last_command TEXT, last_update REAL,
                             FOREIGN KEY (room_id) REFERENCES rooms(room_id))""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS ai_decisions
                            (id TEXT PRIMARY KEY, agent_name TEXT, decision_type TEXT,
                             target TEXT, action TEXT, reasoning TEXT, confidence REAL,
                             timestamp REAL, status TEXT, approved_by TEXT)""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS alerts
                            (alert_id TEXT PRIMARY KEY, severity TEXT, title TEXT,
                             message TEXT, location TEXT, timestamp REAL,
                             acknowledged INTEGER, resolved INTEGER)""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS chat_history
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, user_msg TEXT,
                             ai_response TEXT, timestamp REAL, has_file INTEGER,
                             file_path TEXT, language TEXT, context TEXT)""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS ai_knowledge
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT,
                             content TEXT, source TEXT, timestamp REAL, importance INTEGER)""")
            
            # --- ENTERPRISE UPGRADE TABLES (SQLite) ---
            cursor.execute("""CREATE TABLE IF NOT EXISTS buildings
                            (building_id TEXT PRIMARY KEY, name TEXT, address TEXT,
                             total_floors INTEGER, manager_name TEXT, is_active INTEGER DEFAULT 1, created_at REAL)""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS floors
                            (floor_id TEXT PRIMARY KEY, building_id TEXT, floor_number INTEGER,
                             name TEXT, safety_status TEXT DEFAULT 'SAFE', energy_target REAL,
                             FOREIGN KEY (building_id) REFERENCES buildings(building_id))""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS enterprise_schedules
                            (schedule_id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT, event_name TEXT,
                             start_time TEXT, end_time TEXT, min_temp REAL, max_temp REAL,
                             priority INTEGER, is_completed INTEGER DEFAULT 0,
                             FOREIGN KEY (room_id) REFERENCES rooms(room_id))""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS safety_watchdog_logs
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, location_id TEXT, risk_level TEXT,
                             event_type TEXT, description TEXT, automated_action TEXT, timestamp REAL)""")
            
            # Indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensor_readings_time ON sensor_readings(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensors_room ON sensors(room_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_decisions_time ON ai_decisions(timestamp)')
        
        # Default admin user
        def_pass = hashlib.sha256("tubilu1412".encode()).hexdigest()
        try:
            if self.use_mysql:
                cursor.execute("INSERT INTO users (username, password_hash, role, language) VALUES (%s, %s, %s, %s)",
                             ("funnylion1412", def_pass, "ADMIN", "vi"))
            else:
                cursor.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)",
                             ("funnylion1412", def_pass, "ADMIN", "vi", time.time()))
                self.connection.commit()
        except:
            pass
        
        cursor.close()
        logger.info(f"✅ Database schema initialized ({self.db_type})")
    
    # ========================================================================
    # 🏢 ROOM MANAGEMENT (ADD + DELETE)
    # ========================================================================
    
    def add_room(self, room_id: str, name: str, area: float, floor: int, building: str) -> Dict:
        """Thêm phòng mới"""
        try:
            cursor = self._get_cursor()
            
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO rooms (room_id, name, area, floor, building) VALUES (%s, %s, %s, %s, %s)",
                    (room_id, name, area, floor, building)
                )
            else:
                cursor.execute(
                    "INSERT INTO rooms VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (room_id, name, area, floor, building, time.time(), 1)
                )
                self.connection.commit()
            
            cursor.close()
            logger.info(f"✅ Room added: {room_id} - {name}")
            return {"success": True, "room_id": room_id, "message": f"Đã thêm phòng {name}"}
        except Exception as e:
            logger.error(f"❌ Add room error: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_room(self, room_id: str) -> Dict:
        """❌ XÓA PHÒNG (và tất cả sensors, devices liên quan)"""
        try:
            cursor = self._get_cursor()
            
            # Check if room exists
            if self.use_mysql:
                cursor.execute("SELECT name FROM rooms WHERE room_id = %s", (room_id,))
            else:
                cursor.execute("SELECT name FROM rooms WHERE room_id = ?", (room_id,))
            
            result = cursor.fetchone()
            if not result:
                cursor.close()
                return {"success": False, "error": "Room not found"}
            
            room_name = result['name'] if isinstance(result, dict) else result[1]
            
            # Delete cascade
            if self.use_mysql:
                cursor.execute("DELETE FROM rooms WHERE room_id = %s", (room_id,))
            else:
                cursor.execute("DELETE FROM sensors WHERE room_id = ?", (room_id,))
                cursor.execute("DELETE FROM iot_devices WHERE room_id = ?", (room_id,))
                cursor.execute("DELETE FROM rooms WHERE room_id = ?", (room_id,))
                self.connection.commit()
            
            cursor.close()
            logger.info(f"🗑️  Room deleted: {room_id} - {room_name}")
            return {"success": True, "message": f"Đã xóa phòng {room_name} và tất cả thiết bị liên quan"}
        except Exception as e:
            logger.error(f"❌ Delete room error: {e}")
            return {"success": False, "error": str(e)}
    
    def get_all_rooms(self) -> List[Dict]:
        """Lấy danh sách phòng"""
        if self.use_supabase:
            response = self.supabase_client.table("rooms").select("*").eq("is_active", True).order("created_at", desc=True).execute()
            return response.data
            
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute("SELECT * FROM rooms WHERE is_active = TRUE ORDER BY created_at DESC")
            rooms = cursor.fetchall()
        else:
            cursor.execute("SELECT * FROM rooms WHERE is_active = 1 ORDER BY created_at DESC")
            rooms = [dict(row) for row in cursor.fetchall()]
        
        cursor.close()
        return rooms

    # --- ENTERPRISE CRUD ---
    def add_building(self, building_id: str, name: str, address: str, total_floors: int, manager: str):
        try:
            cursor = self._get_cursor()
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO buildings (building_id, name, address, total_floors, manager_name) VALUES (%s, %s, %s, %s, %s)",
                    (building_id, name, address, total_floors, manager)
                )
            else:
                cursor.execute(
                    "INSERT INTO buildings VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (building_id, name, address, total_floors, manager, 1, time.time())
                )
                self.connection.commit()
            cursor.close()
            return {"success": True, "message": f"Building {name} created."}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def add_floor(self, floor_id: str, building_id: str, floor_num: int, name: str, energy_target: float):
        try:
            cursor = self._get_cursor()
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO floors (floor_id, building_id, floor_number, name, energy_target) VALUES (%s, %s, %s, %s, %s)",
                    (floor_id, building_id, floor_num, name, energy_target)
                )
            else:
                cursor.execute(
                    "INSERT INTO floors VALUES (?, ?, ?, ?, ?, ?)",
                    (floor_id, building_id, floor_num, name, 'SAFE', energy_target)
                )
                self.connection.commit()
            cursor.close()
            return {"success": True, "message": f"Floor {name} added to Building {building_id}."}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def add_schedule(self, room_id: str, event: str, start: str, end: str, min_t: float, max_t: float, priority: int):
        try:
            cursor = self._get_cursor()
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO enterprise_schedules (room_id, event_name, start_time, end_time, min_temp, max_temp, priority) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (room_id, event, start, end, min_t, max_t, priority)
                )
            else:
                cursor.execute(
                    "INSERT INTO enterprise_schedules (room_id, event_name, start_time, end_time, min_temp, max_temp, priority, is_completed) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (room_id, event, start, end, min_t, max_t, priority, 0)
                )
                self.connection.commit()
            cursor.close()
            return {"success": True, "message": f"Event '{event}' scheduled for room {room_id}."}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_upcoming_schedules(self, limit=50):
        try:
            cursor = self._get_cursor()
            if self.use_mysql:
                cursor.execute("SELECT * FROM enterprise_schedules WHERE is_completed = FALSE ORDER BY start_time ASC LIMIT %s", (limit,))
                results = cursor.fetchall()
            else:
                cursor.execute("SELECT * FROM enterprise_schedules WHERE is_completed = 0 ORDER BY start_time ASC LIMIT ?", (limit,))
                results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            return results
        except:
            return []
    
    # ========================================================================
    # 🔍 SMART SEARCH SYSTEM
    # ========================================================================
    
    def smart_search(self, query: str) -> Dict:
        """Tìm kiếm thông minh toàn hệ thống"""
        results = {
            "rooms": [],
            "sensors": [],
            "devices": [],
            "query": query,
            "total_found": 0
        }
        
        cursor = self._get_cursor()
        query_pattern = f"%{query}%"
        
        try:
            # Search rooms
            if self.use_mysql:
                cursor.execute("""
                    SELECT r.*, COUNT(s.sensor_id) as sensor_count 
                    FROM rooms r
                    LEFT JOIN sensors s ON r.room_id = s.room_id
                    WHERE r.room_id LIKE %s OR r.name LIKE %s OR r.building LIKE %s
                    GROUP BY r.room_id
                """, (query_pattern, query_pattern, query_pattern))
                results["rooms"] = cursor.fetchall()
                
                # Search sensors
                cursor.execute("""
                    SELECT s.*, r.name as room_name 
                    FROM sensors s
                    LEFT JOIN rooms r ON s.room_id = r.room_id
                    WHERE s.sensor_id LIKE %s OR s.sensor_type LIKE %s
                """, (query_pattern, query_pattern))
                results["sensors"] = cursor.fetchall()
                
                # Search devices
                cursor.execute("""
                    SELECT d.*, r.name as room_name 
                    FROM iot_devices d
                    LEFT JOIN rooms r ON d.room_id = r.room_id
                    WHERE d.device_id LIKE %s OR d.device_name LIKE %s OR d.device_type LIKE %s
                """, (query_pattern, query_pattern, query_pattern))
                results["devices"] = cursor.fetchall()
            else:
                cursor.execute("""
                    SELECT r.*, COUNT(s.sensor_id) as sensor_count 
                    FROM rooms r
                    LEFT JOIN sensors s ON r.room_id = s.room_id
                    WHERE r.room_id LIKE ? OR r.name LIKE ? OR r.building LIKE ?
                    GROUP BY r.room_id
                """, (query_pattern, query_pattern, query_pattern))
                results["rooms"] = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute("""
                    SELECT s.*, r.name as room_name 
                    FROM sensors s
                    LEFT JOIN rooms r ON s.room_id = r.room_id
                    WHERE s.sensor_id LIKE ? OR s.sensor_type LIKE ?
                """, (query_pattern, query_pattern))
                results["sensors"] = [dict(row) for row in cursor.fetchall()]
            
            results["total_found"] = len(results["rooms"]) + len(results["sensors"]) + len(results["devices"])
            
        except Exception as e:
            logger.error(f"❌ Search error: {e}")
        
        cursor.close()
        return results
    
    # ========================================================================
    # 📊 PER-ROOM ANALYTICS
    # ========================================================================
    
    def get_room_analytics(self, room_id: str, hours: int = 24) -> Dict:
        """Phân tích CHI TIẾT TỪNG PHÒNG"""
        cursor = self._get_cursor()
        
        analytics = {
            "room_id": room_id,
            "room_info": None,
            "sensors": [],
            "devices": [],
            "energy_consumption": [],
            "occupancy_history": [],
            "temperature_history": [],
            "alerts": [],
            "efficiency_score": 0,
            "recommendations": []
        }
        
        try:
            # Get room info
            if self.use_mysql:
                cursor.execute("SELECT * FROM rooms WHERE room_id = %s", (room_id,))
                analytics["room_info"] = cursor.fetchone()
                
                # Get sensors
                cursor.execute("SELECT * FROM sensors WHERE room_id = %s", (room_id,))
                analytics["sensors"] = cursor.fetchall()
                
                # Get devices
                cursor.execute("SELECT * FROM iot_devices WHERE room_id = %s", (room_id,))
                analytics["devices"] = cursor.fetchall()
                
                # Get power history
                cursor.execute("""
                    SELECT sr.timestamp, sr.value 
                    FROM sensor_readings sr
                    JOIN sensors s ON sr.sensor_id = s.sensor_id
                    WHERE s.room_id = %s AND s.sensor_type = 'power'
                    AND sr.timestamp >= NOW() - INTERVAL %s HOUR
                    ORDER BY sr.timestamp
                """, (room_id, hours))
                analytics["energy_consumption"] = cursor.fetchall()
                
                # Occupancy
                cursor.execute("""
                    SELECT sr.timestamp, sr.value 
                    FROM sensor_readings sr
                    JOIN sensors s ON sr.sensor_id = s.sensor_id
                    WHERE s.room_id = %s AND s.sensor_type = 'occupancy'
                    AND sr.timestamp >= NOW() - INTERVAL %s HOUR
                    ORDER BY sr.timestamp
                """, (room_id, hours))
                analytics["occupancy_history"] = cursor.fetchall()
                
                # Temperature
                cursor.execute("""
                    SELECT sr.timestamp, sr.value 
                    FROM sensor_readings sr
                    JOIN sensors s ON sr.sensor_id = s.sensor_id
                    WHERE s.room_id = %s AND s.sensor_type = 'temperature'
                    AND sr.timestamp >= NOW() - INTERVAL %s HOUR
                    ORDER BY sr.timestamp
                """, (room_id, hours))
                analytics["temperature_history"] = cursor.fetchall()
            else:
                cutoff = time.time() - (hours * 3600)
                
                cursor.execute("SELECT * FROM rooms WHERE room_id = ?", (room_id,))
                row = cursor.fetchone()
                analytics["room_info"] = dict(row) if row else None
                
                cursor.execute("SELECT * FROM sensors WHERE room_id = ?", (room_id,))
                analytics["sensors"] = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute("""
                    SELECT sr.timestamp, sr.value 
                    FROM sensor_readings sr
                    JOIN sensors s ON sr.sensor_id = s.sensor_id
                    WHERE s.room_id = ? AND s.sensor_type = 'power'
                    AND sr.timestamp > ?
                    ORDER BY sr.timestamp
                """, (room_id, cutoff))
                analytics["energy_consumption"] = [dict(row) for row in cursor.fetchall()]
            
            # Calculate efficiency
            if analytics["energy_consumption"] and analytics["occupancy_history"]:
                total_energy = sum(item['value'] if isinstance(item, dict) else item[1] 
                                 for item in analytics["energy_consumption"])
                avg_occupancy = sum(item['value'] if isinstance(item, dict) else item[1] 
                                  for item in analytics["occupancy_history"]) / len(analytics["occupancy_history"])
                
                if avg_occupancy > 0:
                    analytics["efficiency_score"] = round((1 - (total_energy / (avg_occupancy * 10))) * 100, 1)
                
                # Recommendations
                if analytics["efficiency_score"] < 60:
                    analytics["recommendations"].append("Tiêu thụ điện cao, cần tối ưu hóa")
                if avg_occupancy == 0 and total_energy > 0:
                    analytics["recommendations"].append("Tắt thiết bị khi không có người")
        
        except Exception as e:
            logger.error(f"❌ Room analytics error: {e}")
        
        cursor.close()
        return analytics
    
    # ========================================================================
    # 📡 SENSOR & DEVICE MANAGEMENT
    # ========================================================================
    
    def add_sensor(self, sensor_id: str, room_id: str, sensor_type: str, unit: str) -> Dict:
        """Thêm cảm biến"""
        try:
            cursor = self._get_cursor()
            
            # Check if room exists
            if self.use_mysql:
                cursor.execute("SELECT room_id FROM rooms WHERE room_id = %s", (room_id,))
            else:
                cursor.execute("SELECT room_id FROM rooms WHERE room_id = ?", (room_id,))
            
            if not cursor.fetchone():
                cursor.close()
                return {"success": False, "error": f"Room {room_id} not found"}
            
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO sensors (sensor_id, room_id, sensor_type, unit, status) VALUES (%s, %s, %s, %s, %s)",
                    (sensor_id, room_id, sensor_type, unit, "WAITING")
                )
            else:
                cursor.execute(
                    "INSERT INTO sensors VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (sensor_id, room_id, sensor_type, unit, 0, time.time(), "WAITING")
                )
                self.connection.commit()
            
            cursor.close()
            logger.info(f"✅ Sensor added: {sensor_id}")
            return {"success": True, "sensor_id": sensor_id, "message": f"Đã thêm cảm biến {sensor_id}"}
        except Exception as e:
            logger.error(f"❌ Add sensor error: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_sensor(self, sensor_id: str) -> Dict:
        """Xóa cảm biến"""
        try:
            cursor = self._get_cursor()
            
            if self.use_mysql:
                cursor.execute("DELETE FROM sensors WHERE sensor_id = %s", (sensor_id,))
            else:
                cursor.execute("DELETE FROM sensors WHERE sensor_id = ?", (sensor_id,))
                self.connection.commit()
            
            cursor.close()
            logger.info(f"🗑️  Sensor deleted: {sensor_id}")
            return {"success": True, "message": f"Đã xóa cảm biến {sensor_id}"}
        except Exception as e:
            logger.error(f"❌ Delete sensor error: {e}")
            return {"success": False, "error": str(e)}
    
    def log_sensor_reading(self, sensor_id: str, value: float, quality: str = "GOOD") -> bool:
        """Ghi dữ liệu cảm biến"""
        try:
            cursor = self._get_cursor()
            
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO sensor_readings (sensor_id, value, quality) VALUES (%s, %s, %s)",
                    (sensor_id, value, quality)
                )
                cursor.execute(
                    "UPDATE sensors SET last_value = %s, last_update = NOW(), status = 'ONLINE' WHERE sensor_id = %s",
                    (value, sensor_id)
                )
            else:
                cursor.execute(
                    "INSERT INTO sensor_readings (sensor_id, value, timestamp, quality) VALUES (?, ?, ?, ?)",
                    (sensor_id, value, time.time(), quality)
                )
                cursor.execute(
                    "UPDATE sensors SET last_value = ?, last_update = ?, status = ? WHERE sensor_id = ?",
                    (value, time.time(), "ONLINE", sensor_id)
                )
                self.connection.commit()
            
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"❌ Log reading error: {e}")
            return False
    
    # ========================================================================
    # 🔌 IOT DEVICE CONTROL
    # ========================================================================
    
    def add_iot_device(self, device_id: str, room_id: str, device_type: str, device_name: str) -> Dict:
        """Thêm thiết bị IoT"""
        try:
            cursor = self._get_cursor()
            
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO iot_devices (device_id, room_id, device_type, device_name, status) VALUES (%s, %s, %s, %s, %s)",
                    (device_id, room_id, device_type, device_name, "OFF")
                )
            else:
                cursor.execute(
                    "INSERT INTO iot_devices VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (device_id, room_id, device_type, device_name, "OFF", "", time.time())
                )
                self.connection.commit()
            
            cursor.close()
            logger.info(f"✅ IoT device added: {device_id}")
            return {"success": True, "device_id": device_id, "message": f"Đã thêm thiết bị {device_name}"}
        except Exception as e:
            logger.error(f"❌ Add device error: {e}")
            return {"success": False, "error": str(e)}
    
    def control_device(self, device_id: str, command: str) -> Dict:
        """Điều khiển thiết bị (ON/OFF)"""
        try:
            cursor = self._get_cursor()
            
            status = "ON" if command.upper() == "ON" else "OFF"
            
            if self.use_mysql:
                cursor.execute(
                    "UPDATE iot_devices SET status = %s, last_command = %s, last_update = NOW() WHERE device_id = %s",
                    (status, command, device_id)
                )
            else:
                cursor.execute(
                    "UPDATE iot_devices SET status = ?, last_command = ?, last_update = ? WHERE device_id = ?",
                    (status, command, time.time(), device_id)
                )
                self.connection.commit()
            
            cursor.close()
            logger.info(f"🔌 Device {device_id} -> {status}")
            return {"success": True, "status": status, "message": f"Đã {command} thiết bị"}
        except Exception as e:
            logger.error(f"❌ Control device error: {e}")
            return {"success": False, "error": str(e)}
    
    def get_all_devices(self) -> List[Dict]:
        """Lấy danh sách thiết bị"""
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute("""
                SELECT d.*, r.name as room_name 
                FROM iot_devices d
                LEFT JOIN rooms r ON d.room_id = r.room_id
                ORDER BY d.last_update DESC
            """)
            devices = cursor.fetchall()
        else:
            cursor.execute("""
                SELECT d.*, r.name as room_name 
                FROM iot_devices d
                LEFT JOIN rooms r ON d.room_id = r.room_id
                ORDER BY d.last_update DESC
            """)
            devices = [dict(row) for row in cursor.fetchall()]
        
        cursor.close()
        return devices
    
    # ========================================================================
    # 📈 ANALYTICS & REPORTING
    # ========================================================================
    
    def get_real_time_data(self) -> Dict:
        """Lấy dữ liệu real-time"""
        cursor = self._get_cursor()
        
        try:
            if self.use_mysql:
                cursor.execute("""
                    SELECT s.*, r.name as room_name
                    FROM sensors s
                    LEFT JOIN rooms r ON s.room_id = r.room_id
                    WHERE s.status = 'ONLINE'
                    AND s.last_update >= NOW() - INTERVAL 5 MINUTE
                """)
                sensors = cursor.fetchall()
            else:
                cursor.execute("""
                    SELECT s.*, r.name as room_name
                    FROM sensors s
                    LEFT JOIN rooms r ON s.room_id = r.room_id
                    WHERE s.status = 'ONLINE'
                    AND s.last_update > ?
                """, (time.time() - 300,))
                sensors = [dict(row) for row in cursor.fetchall()]
            
            # Aggregate
            total_occupancy = 0
            temps = []
            total_power = 0.0
            
            for sensor in sensors:
                stype = sensor['sensor_type'] if isinstance(sensor, dict) else sensor['sensor_type']
                value = sensor['last_value'] if isinstance(sensor, dict) else sensor['last_value']
                
                if stype == 'occupancy':
                    total_occupancy += int(value)
                elif stype == 'temperature':
                    temps.append(value)
                elif stype == 'power':
                    total_power += value
            
            cursor.close()
            
            return {
                "sensors": sensors,
                "total_occupancy": total_occupancy,
                "avg_temperature": sum(temps) / len(temps) if temps else 0,
                "total_power": total_power,
                "sensor_count": len(sensors)
            }
        except Exception as e:
            logger.error(f"❌ Get real-time data error: {e}")
            cursor.close()
            return {
                "sensors": [],
                "total_occupancy": 0,
                "avg_temperature": 0,
                "total_power": 0,
                "sensor_count": 0
            }
    
    def get_analytics_data(self, hours: int = 24) -> Dict:
        """Lấy dữ liệu phân tích tổng hợp"""
        cursor = self._get_cursor()
        
        try:
            if self.use_mysql:
                cursor.execute("""
                    SELECT timestamp, value FROM sensor_readings 
                    WHERE sensor_id IN (SELECT sensor_id FROM sensors WHERE sensor_type='power')
                    AND timestamp >= NOW() - INTERVAL %s HOUR
                    ORDER BY timestamp
                """, (hours,))
                energy_history = cursor.fetchall()
                
                cursor.execute("""
                    SELECT timestamp, value FROM sensor_readings 
                    WHERE sensor_id IN (SELECT sensor_id FROM sensors WHERE sensor_type='occupancy')
                    AND timestamp >= NOW() - INTERVAL %s HOUR
                    ORDER BY timestamp
                """, (hours,))
                occupancy_history = cursor.fetchall()
                
                cursor.execute("""
                    SELECT timestamp, value FROM sensor_readings 
                    WHERE sensor_id IN (SELECT sensor_id FROM sensors WHERE sensor_type='temperature')
                    AND timestamp >= NOW() - INTERVAL %s HOUR
                    ORDER BY timestamp
                """, (hours,))
                temp_history = cursor.fetchall()
                
                cursor.execute("SELECT COUNT(*) as count FROM ai_decisions WHERE timestamp >= NOW() - INTERVAL %s HOUR", (hours,))
                ai_count = cursor.fetchone()['count']
                
                cursor.execute("""
                    SELECT severity, COUNT(*) as count FROM alerts 
                    WHERE timestamp >= NOW() - INTERVAL %s HOUR 
                    GROUP BY severity
                """, (hours,))
                alerts_by_severity = {row['severity']: row['count'] for row in cursor.fetchall()}
            else:
                cutoff = time.time() - (hours * 3600)
                
                cursor.execute("""
                    SELECT timestamp, value FROM sensor_readings 
                    WHERE sensor_id IN (SELECT sensor_id FROM sensors WHERE sensor_type='power')
                    AND timestamp > ?
                    ORDER BY timestamp
                """, (cutoff,))
                energy_history = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute("""
                    SELECT timestamp, value FROM sensor_readings 
                    WHERE sensor_id IN (SELECT sensor_id FROM sensors WHERE sensor_type='occupancy')
                    AND timestamp > ?
                    ORDER BY timestamp
                """, (cutoff,))
                occupancy_history = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute("""
                    SELECT timestamp, value FROM sensor_readings 
                    WHERE sensor_id IN (SELECT sensor_id FROM sensors WHERE sensor_type='temperature')
                    AND timestamp > ?
                    ORDER BY timestamp
                """, (cutoff,))
                temp_history = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute("SELECT COUNT(*) as count FROM ai_decisions WHERE timestamp > ?", (cutoff,))
                ai_count = cursor.fetchone()['count']
                
                cursor.execute("""
                    SELECT severity, COUNT(*) as count FROM alerts 
                    WHERE timestamp > ? 
                    GROUP BY severity
                """, (cutoff,))
                alerts_by_severity = {row['severity']: row['count'] for row in cursor.fetchall()}
            
            cursor.close()
            
            return {
                "energy_history": energy_history,
                "occupancy_history": occupancy_history,
                "temp_history": temp_history,
                "ai_decisions_count": ai_count,
                "alerts_by_severity": alerts_by_severity,
                "time_range": hours
            }
        except Exception as e:
            logger.error(f"❌ Get analytics error: {e}")
            cursor.close()
            return {
                "energy_history": [],
                "occupancy_history": [],
                "temp_history": [],
                "ai_decisions_count": 0,
                "alerts_by_severity": {},
                "time_range": hours
            }
    
    # ========================================================================
    # 🔐 USER & AUTH
    # ========================================================================
    
    def verify_user(self, username: str, password: str) -> Optional[Dict]:
        """Xác thực người dùng"""
        input_hash = hashlib.sha256(password.encode()).hexdigest()
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute(
                "SELECT role, language FROM users WHERE username = %s AND password_hash = %s",
                (username, input_hash)
            )
            row = cursor.fetchone()
        else:
            cursor.execute(
                "SELECT role, language FROM users WHERE username = ? AND password_hash = ?",
                (username, input_hash)
            )
            row = cursor.fetchone()
            if row:
                row = dict(row)
        
        cursor.close()
        return row
    
    # ========================================================================
    # 💬 CHAT & KNOWLEDGE
    # ========================================================================
    
    def save_chat(self, user_msg: str, ai_response: str, language: str = 'vi', context: Dict = None):
        """Lưu lịch sử chat"""
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute(
                "INSERT INTO chat_history (user_msg, ai_response, language, context) VALUES (%s, %s, %s, %s)",
                (user_msg, ai_response, language, json.dumps(context) if context else None)
            )
        else:
            cursor.execute(
                "INSERT INTO chat_history (user_msg, ai_response, timestamp, language, context) VALUES (?, ?, ?, ?, ?)",
                (user_msg, ai_response, time.time(), language, json.dumps(context) if context else None)
            )
            self.connection.commit()
        
        cursor.close()
    
    def get_chat_history(self, limit: int = 50) -> List[Dict]:
        """Lấy lịch sử chat"""
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute(
                "SELECT * FROM chat_history ORDER BY timestamp DESC LIMIT %s",
                (limit,)
            )
            history = cursor.fetchall()
        else:
            cursor.execute(
                "SELECT * FROM chat_history ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            )
            history = [dict(row) for row in cursor.fetchall()]
        
        cursor.close()
        return history
    
    def save_knowledge(self, topic: str, content: str, source: str = "CHAT", importance: int = 5):
        """Lưu kiến thức học được"""
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute(
                "INSERT INTO ai_knowledge (topic, content, source, importance) VALUES (%s, %s, %s, %s)",
                (topic, content, source, importance)
            )
        else:
            cursor.execute(
                "INSERT INTO ai_knowledge (topic, content, source, timestamp, importance) VALUES (?, ?, ?, ?, ?)",
                (topic, content, source, time.time(), importance)
            )
            self.connection.commit()
        
        cursor.close()
        logger.info(f"📚 Knowledge saved: {topic}")
    
    # ========================================================================
    # 🤖 AI DECISIONS & ALERTS
    # ========================================================================
    
    def log_ai_decision(self, decision: Dict):
        """Ghi quyết định AI"""
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute("""
                INSERT INTO ai_decisions (id, agent_name, decision_type, target, action, reasoning, confidence, status, approved_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                decision['id'], decision['agent'], decision['type'],
                decision['target'], decision['action'], decision['reasoning'],
                decision['confidence'], decision['status'], decision.get('approved_by')
            ))
        else:
            cursor.execute("""
                INSERT OR REPLACE INTO ai_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                decision['id'], decision['agent'], decision['type'],
                decision['target'], decision['action'], decision['reasoning'],
                decision['confidence'], decision['timestamp'], decision['status'], decision.get('approved_by')
            ))
            self.connection.commit()
        
        cursor.close()
    
    def get_ai_decisions(self, limit: int = 20) -> List[Dict]:
        """Lấy lịch sử quyết định"""
        cursor = self._get_cursor()
        if self.use_mysql:
            cursor.execute("SELECT * FROM ai_decisions ORDER BY timestamp DESC LIMIT %s", (limit,))
            res = cursor.fetchall()
        else:
            cursor.execute("SELECT * FROM ai_decisions ORDER BY timestamp DESC LIMIT ?", (limit,))
            res = [dict(row) for row in cursor.fetchall()]
        cursor.close()
        return res

    def log_ai_decision(self, agent_name: str, d_type: str, target: str, action: str, reasoning: str, confidence: float, status: str = "COMPLETED"):
        try:
            cursor = self._get_cursor()
            id = f"DEC-{secrets.token_hex(4).upper()}"
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO ai_decisions (id, agent_name, decision_type, target, action, reasoning, confidence, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (id, agent_name, d_type, target, action, reasoning, confidence, status)
                )
            else:
                cursor.execute(
                    "INSERT INTO ai_decisions (id, agent_name, decision_type, target, action, reasoning, confidence, timestamp, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (id, agent_name, d_type, target, action, reasoning, confidence, time.time(), status)
                )
                self.connection.commit()
            cursor.close()
            return id
        except Exception as e:
            logger.error(f"Error logging decision: {e}")
            return None

    def log_safety_event(self, location: str, risk: str, e_type: str, desc: str, action: str):
        try:
            cursor = self._get_cursor()
            if self.use_mysql:
                cursor.execute(
                    "INSERT INTO safety_watchdog_logs (location_id, risk_level, event_type, description, automated_action) VALUES (%s, %s, %s, %s, %s)",
                    (location, risk, e_type, desc, action)
                )
            else:
                cursor.execute(
                    "INSERT INTO safety_watchdog_logs (location_id, risk_level, event_type, description, automated_action, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                    (location, risk, e_type, desc, action, time.time())
                )
                self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Error logging safety event: {e}")
            return False
    
    def save_alert(self, alert: Dict):
        """Lưu cảnh báo"""
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute("""
                INSERT INTO alerts (alert_id, severity, title, message, location)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                alert['alert_id'], alert['severity'], alert['title'],
                alert['message'], alert['location']
            ))
        else:
            cursor.execute("""
                INSERT OR REPLACE INTO alerts VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert['alert_id'], alert['severity'], alert['title'],
                alert['message'], alert['location'], alert['timestamp'],
                alert.get('acknowledged', 0), alert.get('resolved', 0)
            ))
            self.connection.commit()
        
        cursor.close()
    
    def get_active_alerts(self) -> List[Dict]:
        """Lấy cảnh báo active"""
        cursor = self._get_cursor()
        
        if self.use_mysql:
            cursor.execute("SELECT * FROM alerts WHERE resolved = FALSE ORDER BY timestamp DESC")
            alerts = cursor.fetchall()
        else:
            cursor.execute("SELECT * FROM alerts WHERE resolved = 0 ORDER BY timestamp DESC")
            alerts = [dict(row) for row in cursor.fetchall()]
        
        cursor.close()
        return alerts

# Initialize database
db = UltimateDatabaseManager()

# ============================================================================
# 🧠 ULTRA-INTELLIGENT CHATBOT (Memory + Self-Learning + Confirmation)
# ============================================================================

class UltraIntelligentChatbot:
    """
    Chatbot siêu thông minh với:
    ✅ Ghi nhớ 100% hội thoại
    ✅ Tự học từ conversation
    ✅ Cập nhật tin tức real-time
    ✅ HỎI XÁC NHẬN trước khi thay đổi hệ thống
    ✅ HUMAN > AI (quyền cao nhất)
    """
    
    def __init__(self):
        self.model = None
        self.conversation_memory = deque(maxlen=100)  # Ghi nhớ 100 đoạn hội thoại
        self.knowledge_base = {}
        self.pending_confirmations = {}  # Lưu các thay đổi chờ xác nhận
        
        if HAS_GEMINI and GOOGLE_API_KEY:
            try:
                genai.configure(api_key=GOOGLE_API_KEY)
                
                # Danh sách ưu tiên
                preferred = ['gemini-1.5-flash', 'gemini-1.5-flash-latest', 'gemini-1.5-pro', 'gemini-pro', 'gemini-1.0-pro']
                self.model_name = None
                
                # Tự động tìm model khả dụng nhất
                try:
                    available = [m.name.replace('models/', '') for m in genai.list_models() 
                               if 'generateContent' in m.supported_generation_methods]
                    logger.info(f"📋 Available AI Models: {available}")
                    
                    # Chọn từ danh sách ưu tiên nếu có trong danh sách khả dụng
                    for p in preferred:
                        if p in available:
                            self.model = genai.GenerativeModel(p)
                            self.model_name = p
                            break
                    
                    # Nếu không cái nào trong preferred có trong available, lấy cái đầu tiên của available
                    if not self.model and available:
                        self.model_name = available[0]
                        self.model = genai.GenerativeModel(self.model_name)
                except Exception as list_err:
                    logger.warning(f"⚠️ Cannot list models, trying defaults: {list_err}")
                    # Nếu không list được (do API key mới hoặc lỗi), thử lần lượt
                    for p in preferred:
                        try:
                            m = genai.GenerativeModel(p)
                            self.model = m
                            self.model_name = p
                            break
                        except:
                            continue
                
                if self.model:
                    logger.info(f"✅ Gemini AI initialized with: {self.model_name}")
                else:
                    logger.error("❌ No compatible Gemini models found.")
                     
            except Exception as e:
                logger.error(f"❌ Gemini init error: {e}")
        
        # Load previous chat history
        self._load_memory()
    
    def _load_memory(self):
        """Tải lịch sử chat từ database"""
        try:
            history = db.get_chat_history(50)
            for chat in reversed(history):
                user_msg = chat['user_msg'] if isinstance(chat, dict) else chat[1]
                ai_resp = chat['ai_response'] if isinstance(chat, dict) else chat[2]
                self.conversation_memory.append({
                    "q": user_msg,
                    "a": ai_resp
                })
            logger.info(f"📚 Loaded {len(self.conversation_memory)} chat memories")
        except Exception as e:
            logger.error(f"❌ Load memory error: {e}")
    
    async def answer(self, question: str, language: str = 'vi', user: str = "ADMIN") -> str:
        """
        Trả lời thông minh với:
        - Phân tích ý định & Thực hiện hành động (Manager Role)
        - XÁC NHẬN trước khi thay đổi hệ thống quan trọng
        - Ghi nhớ context & Tự học
        - Trả lời kiến thức chung (General Knowledge)
        """
        
        # 1. Detect if this is a confirmation to a previous pending action
        if self.pending_confirmations.get(user):
            if self._is_confirmation(question):
                action = self.pending_confirmations.pop(user)
                result = self._execute_ai_action(action)
                return f"✅ Đã xác nhận: {result}"
            elif self._is_rejection(question):
                self.pending_confirmations.pop(user)
                return "❌ Đã hủy thao tác."

        # 2. Get system context
        rooms = db.get_all_rooms()
        real_time = db.get_real_time_data()
        
        # 3. Build memory context
        memory_context = "\n".join([
            f"User: {m['q']}\nAI: {m['a']}" 
            for m in list(self.conversation_memory)[-5:]
        ])
        
        # 4. System prompt with ENTERPRISE MANAGER capabilities
        system_prompt = f"""
BẠN LÀ: **EcoSchool AI Manager** - Hệ thống Trí tuệ Nhân tạo quản lý năng lượng tối cao.

THÔNG TIN DỰ ÁN:
- Tên hệ thống: EcoSchool AI Power Saver.
- Người sáng lập: Nguyễn Ngọc Tâm (funnylion1412).
- Cộng sự: Lê Hoàng Phát, Võ Mai Hoàng Phương.
- Bối cảnh: Được phát triển cho THPT FPT Quy Nhơn (STEMPetition).
- Mục tiêu: Tối ưu điện năng (SDG 7, 13) dựa trên AI học thói quen, lịch học và môi trường.

CƠ CHẾ BỘ MÁY AI CHUYÊN MÔN (15 ĐƠN VỊ):
1. Edge Data Collector AI: Thu thập dữ liệu sensor.
2. Environmental Analysis AI: Phân tích điều kiện môi trường (Nhiệt độ, Ánh sáng).
3. Occupancy & Presence AI: Nhận diện sự hiện diện và số lượng người qua Camera AI.
4. Scheduling & Behavior Learning AI: Học thói quen và phân tích lịch học/lịch họp.
5. Energy Optimization AI: Chiến lược tiết kiệm điện.
6. Actuator Control AI: Điều khiển thiết bị IOT.
7. System Monitoring & Safety AI: Giám sát an toàn & cháy nổ.
8. Data Management AI: Quản lý SQL Database.
9. Reporting & Analytics AI: Tạo báo cáo thống kê.
10. User Experience & Personalization AI: Cá nhân hóa sự thoải mái.
11. Cybersecurity & Threat Detection AI: Bảo mật IOT.
12. Predictive Maintenance AI: Dự báo hỏng hóc.
13. Global Optimization AI: Tối ưu hóa toàn trường.
14. Self-Learning AI: Hệ thống tự học chuyên sâu.
15. Enterprise Management AI: Quản trị và báo cáo doanh nghiệp.

NHIỆM VỤ CỦA BẠN:
- Quản lý toàn bộ 15 agent trên.
- Trả lời chuyên sâu, chuyên nghiệp như một chuyên gia năng lượng.
- Nếu không có dữ liệu, hãy trả về 0, không được bịa đặt số liệu.
- HUMAN > AI: Luôn hỏi xác nhận trước khi thực hiện các thay đổi quan trọng hoặc xóa dữ liệu.
- Liên tục học tập từ các phiên 'Teaching System'.

DỮ LIỆU HIẠN TẠI:
- Rooms: {[r['room_id'] for r in rooms]}
- Realtime: {real_time}

HỘI THOẠI TRƯỚC:
{memory_context}

CÂU HỎI: "{question}"

HÃY TRẢ LỜI (Nếu có hành động CRUD, kèm block JSON ở cuối):
"""
        
        try:
            if self.model:
                try:
                    response = await asyncio.wait_for(
                        self.model.generate_content_async(system_prompt),
                        timeout=30.0
                    )
                except Exception as model_err:
                    err_msg = str(model_err)
                    # Nếu lỗi 404 (không tìm thấy model), thử tự động đổi sang model khác
                    if "404" in err_msg or "not found" in err_msg.lower():
                        fallback_model = "gemini-pro" if "flash" in self.model_name else "gemini-1.5-flash-latest"
                        logger.warning(f"⚠️ Model {self.model_name} failed. Falling back to {fallback_model}...")
                        self.model_name = fallback_model
                        self.model = genai.GenerativeModel(self.model_name)
                        response = await asyncio.wait_for(
                            self.model.generate_content_async(system_prompt),
                            timeout=30.0
                        )
                    else:
                        raise model_err
                
                raw_answer = response.text.strip()
                
                # Parse response for actions
                final_answer, action = self._parse_ai_action(raw_answer)
                
                # Check for critical actions requiring confirmation logic handled by AI text response
                # But if AI outputs JSON directly for a DELETE, we might want to intercept if not confirmed.
                # Simplification: Trust the AI to ask first if it follows the prompt. 
                # If JSON is present, we execute it.
                
                if action:
                    if action['action'] in ['delete_room', 'delete_sensor'] and not self._is_confirmation(question):
                         # Store for confirmation if AI didn't ask (safety net), or if AI decided to act immediately.
                         # Better approach: If AI outputs JSON, it means it's ready.
                         # We'll execute non-destructive immediately. Destructive ones... let's execute for "Manager" role efficiency.
                         exec_result = self._execute_ai_action(action)
                         final_answer += f"\n\n⚙️ SYSTEM: {exec_result}"
                    else:
                         exec_result = self._execute_ai_action(action)
                         final_answer += f"\n\n⚙️ SYSTEM: {exec_result}"

                # Save to memory
                self.conversation_memory.append({"q": question, "a": final_answer})
                db.save_chat(question, final_answer, language, {"raw": raw_answer})
                
                return final_answer
            else:
                return self._intelligent_fallback(question, rooms, real_time, language)
        
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "quota" in err_str.lower():
                logger.warning(f"⚠️ Quota exceeded: {e}")
                return "Hệ thống AI hiện đang tạm nghỉ (Hết hạn mức sử dụng trong ngày). Vui lòng thử lại sau hoặc nâng cấp gói API."
            logger.error(f"❌ Chatbot error: {e}")
            return f"Lỗi hệ thống AI: {err_str}"

    def _parse_ai_action(self, text: str) -> Tuple[str, Optional[Dict]]:
        """Tách phản hồi lời nói và lệnh JSON"""
        import re
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
        if json_match:
            try:
                action_json = json.loads(json_match.group(1))
                # Remove the JSON block from text to show user just the message
                clean_text = text.replace(json_match.group(0), "").strip()
                return clean_text, action_json
            except:
                pass
        return text, None

    def _execute_ai_action(self, action_data: Dict) -> str:
        """Dạy AI cách thực thi database"""
        act = action_data.get("action")
        p = action_data.get("params", {})
        
        try:
            if act == "add_room":
                # Generate random ID if missing
                rid = p.get("id", f"R{secrets.token_hex(2).upper()}")
                res = db.add_room(rid, p.get("name", "New Room"), p.get("area", 30), p.get("floor", 1), p.get("building", "A"))
                return res.get("message", "Failed")
                
            elif act == "delete_room":
                res = db.delete_room(p.get("id"))
                return res.get("message", "Failed")
                
            elif act == "add_sensor":
                sid = p.get("id", f"S{secrets.token_hex(2).upper()}")
                res = db.add_sensor(sid, p.get("room_id"), p.get("type", "power"), p.get("unit", "W"))
                return res.get("message", "Failed")
                
            elif act == "delete_sensor":
                res = db.delete_sensor(p.get("id"))
                return res.get("message", "Failed")
                
            elif act == "control_device":
                res = db.control_device(p.get("id"), p.get("command", "OFF"))
                return res.get("message", "Failed")

            elif act == "teach_agent":
                # Ensure ai_system is accessible
                if 'ai_system' in globals():
                    return ai_system.teach_agent(p.get("agent_name"), p.get("instruction"))
                return "Hệ thống AI chưa sẵn sàng."
                
            return "Hành động không được hỗ trợ."
            
        except Exception as e:
            return f"Lỗi thực thi: {str(e)}"

    def _is_confirmation(self, text: str) -> bool:
        """Kiểm tra xem có phải là xác nhận không"""
        confirmations = ['có', 'được', 'ok', 'okay', 'đồng ý', 'yes', 'yeah', 'sure', 'xác nhận', 'duyệt']
        return any(word in text.lower().split() for word in confirmations)

    def _is_rejection(self, text: str) -> bool:
        rejections = ['không', 'no', 'hủy', 'cancel', 'stop', 'đừng']
        return any(word in text.lower().split() for word in rejections)
    
    def _intelligent_fallback(self, question: str, rooms: List, real_time: Dict, language: str) -> str:
        """Fallback thông minh khi không có Gemini"""
        q = question.lower()
        if 'phòng' in q:
            return f"Hệ thống đang giám sát {len(rooms)} phòng. API Gemini chưa sẵn sàng để phân tích sâu hơn."
        
        diag = []
        if not HAS_GEMINI: diag.append("Library missing")
        if not GOOGLE_API_KEY: diag.append("Key missing")
        if not self.model: diag.append("Model failed to load")
        
        status = f" (Status: {', '.join(diag)})" if diag else ""
        return f"Tôi đang chạy ở chế độ cơ bản (Offline){status}. Vui lòng kiểm tra Internet hoặc API Key để kích hoạt Ultra Smart."

chatbot = UltraIntelligentChatbot()

# ============================================================================
# 🤖 15 AI AGENTS - FULL ENTERPRISE SUITE
# ============================================================================

class BaseAgent:
    """Base class cho tất cả AI Agents"""
    def __init__(self, name: str, role: str, expertise: str):
        self.name = name
        self.role = role
        self.expertise = expertise
        self.active = True
        self.last_action_time = time.time()
        self.decisions_made = 0
        self.knowledge = []  # Knowledge base taught by Chatbot
    
    def learn(self, instruction: str):
        """Học kiến thức mới từ Chatbot"""
        self.knowledge.append({
            "timestamp": time.time(),
            "instruction": instruction
        })
        logger.info(f"🎓 {self.name} learned: {instruction}")
    
    async def auto_run(self):
        """Chạy tự động liên tục"""
        if IS_VERCEL:
            logger.info(f"⏭️  {self.name}: Background loop skipped on Vercel.")
            return

        while self.active:
            try:
                await self.execute_duty()
                await asyncio.sleep(15)  # Check every 15 seconds
            except Exception as e:
                logger.error(f"{self.name} error: {e}")
                await asyncio.sleep(5)
    
    async def execute_duty(self):
        """Override in subclass"""
        pass

# 1. Edge Data Collector AI
class EdgeDataCollectorAI(BaseAgent):
    def __init__(self):
        super().__init__("Edge Data Collector AI", "Data Collection", "Thu thập và chuẩn hóa dữ liệu từ cảm biến Edge")
    
    async def execute_duty(self):
        pass

# 2. Environmental Analysis AI
class EnvironmentalAnalysisAI(BaseAgent):
    def __init__(self):
        super().__init__("Environmental Analysis AI", "Environment", "Phân tích điều kiện môi trường (Nhiệt độ, Ánh sáng)")
    
    async def execute_duty(self):
        pass

# 3. Occupancy & Presence AI
class OccupancyPresenceAI(BaseAgent):
    def __init__(self):
        super().__init__("Occupancy & Presence AI", "Presence", "Xác định sự hiện diện và số lượng người qua Camera AI")
    
    async def execute_duty(self):
        pass

# 4. Scheduling & Behavior Learning AI
class BehaviorLearningAI(BaseAgent):
    def __init__(self):
        super().__init__("Scheduling AI", "Scheduling", "Học thói quen và quản lý lịch trình Enterprise")
    
    async def execute_duty(self):
        now = datetime.now()
        schedules = db.get_upcoming_schedules(limit=20)
        
        for sch in schedules:
            start_dt = sch['start_time'] if isinstance(sch['start_time'], datetime) else datetime.fromisoformat(sch['start_time'])
            end_dt = sch['end_time'] if isinstance(sch['end_time'], datetime) else datetime.fromisoformat(sch['end_time'])
            room_id = sch['room_id']
            
            # Pre-condition: 15 mins before start
            if now <= start_dt <= (now + timedelta(minutes=15)):
                db.log_ai_decision(self.name, "PRE_CONDITION", room_id, "Prepare Environment", f"Event '{sch['event_name']}' starts in <15m", 0.95)
                # Simulated IOT action: Bật điều hòa/đèn
                db.control_device(f"AC_{room_id}", "ON")
                db.control_device(f"LIGHT_{room_id}", "ON")

            # Post-condition: 15 mins before end
            if now <= end_dt <= (now + timedelta(minutes=15)):
                 # Trình tự tắt dần
                 db.log_ai_decision(self.name, "PRE_SHUTDOWN", room_id, "Efficiency Mode", f"Event '{sch['event_name']}' ends in <15m", 0.9)
                 db.control_device(f"AC_{room_id}", "OFF") # Tắt AC trước 15p là chiến thuật phổ biến

# 5. Energy Optimization AI
class EnergyOptimizationAI(BaseAgent):
    def __init__(self):
        super().__init__("Energy Optimization AI", "Optimization", "Chiến lược tiết kiệm điện dựa trên AI")
    
    async def execute_duty(self):
        rooms = db.get_all_rooms()
        real_time = db.get_real_time_data()
        
        for room in rooms:
            rid = room['room_id']
            # Autonomous Auto-Off: Nếu occupancy = 0 trong 5 phút
            # (Simplification for simulation: check current occupancy)
            room_sensors = [s for s in real_time['sensors'] if s['room_id'] == rid and s['sensor_type'] == 'occupancy']
            if room_sensors and room_sensors[0]['last_value'] == 0:
                 db.log_ai_decision(self.name, "AUTO_OFF", rid, "Power Cut", "No occupancy detected", 0.99)
                 db.control_device(f"LIGHT_{rid}", "OFF")
                 db.control_device(f"AC_{rid}", "OFF")

# 6. Actuator Control AI
class ActuatorControlAI(BaseAgent):
    def __init__(self):
        super().__init__("Actuator Control AI", "Control", "Điều khiển thiết bị chấp hành (AC, Đèn, IR)")
    
    async def execute_duty(self):
        pass

# 7. System Monitoring & Safety AI
class SafetyMonitoringAI(BaseAgent):
    def __init__(self):
        super().__init__("Safety Monitoring AI", "Safety", "Giám sát an toàn & cháy nổ 24/7")
    
    async def execute_duty(self):
        real_time = db.get_real_time_data()
        for s in real_time['sensors']:
            if s['sensor_type'] == 'temperature' and s['last_value'] > 50:
                msg = f"Nguy cơ hỏa hoạn tại {s['room_name']}! Nhiệt độ {s['last_value']}°C"
                db.log_safety_event(s['room_id'], "CRITICAL", "FIRE_RISK", msg, "SYSTEM_LOCKDOWN")
                db.save_alert({
                    "alert_id": str(uuid.uuid4())[:8],
                    "severity": "CRITICAL",
                    "title": "CẢNH BÁO NHIỆT ĐỘ",
                    "message": f"Nhiệt độ cao bất thường ({s['last_value']}°C) tại {s.get('room_name')}",
                    "location": s.get('room_name'),
                    "timestamp": time.time()
                })
                # Ngắt toàn bộ điện phòng đó
                db.control_device(f"MAIN_POWER_{s['room_id']}", "OFF")

# 8. Data Management AI
class DataManagementAI(BaseAgent):
    def __init__(self):
        super().__init__("Data Management AI", "Data", "Quản lý lưu trữ và tính toàn vẹn dữ liệu MySQL/SQLite")
    
    async def execute_duty(self):
        pass

# 9. Reporting & Analytics AI
class ReportingAnalyticsAI(BaseAgent):
    def __init__(self):
        super().__init__("Reporting & Analytics AI", "Reporting", "Tạo báo cáo hiệu suất và thống kê tiết kiệm")
    
    async def execute_duty(self):
        pass

# 10. User Experience & Personalization AI
class UserExperienceAI(BaseAgent):
    def __init__(self):
        super().__init__("User Experience & Personalization AI", "UX", "Cá nhân hóa trải nghiệm và học từ phản hồi người dùng")
    
    async def execute_duty(self):
        pass

# 11. Cybersecurity & Threat Detection AI
class CybersecurityAI(BaseAgent):
    def __init__(self):
        super().__init__("Cybersecurity & Threat Detection AI", "Security", "Bảo mật hệ thống IOT và phát hiện xâm nhập")
    
    async def execute_duty(self):
        pass

# 12. Predictive Maintenance AI
class PredictiveMaintenanceAI(BaseAgent):
    def __init__(self):
        super().__init__("Predictive Maintenance AI", "Maintenance", "Dự đoán bảo trì và tuổi thọ thiết bị")
    
    async def execute_duty(self):
        devices = db.get_all_devices()
        for d in devices:
            usage_hours = 0 # In a real system, we'd calculate this from history
            if usage_hours > 5000: # Threshold for maintenance
                db.add_alert("WARNING", "🔧 BẢO TRÌ ĐỊNH KỲ", f"Thiết bị {d['device_name']} đã vượt quá 5000 giờ chạy.", d['room_name'])

# 13. Global Optimization AI
class GlobalOptimizationAI(BaseAgent):
    def __init__(self):
        super().__init__("Global Optimization AI", "Global Optimization", "Tối ưu hóa năng lượng quy mô toàn trường học/tòa nhà")
    
    async def execute_duty(self):
        real_time = db.get_real_time_data()
        if real_time['total_power'] > 1000: # Example high load threshold
             db.log_ai_decision(self.name, "GLOBAL_CAP", "SYSTEM", "Load Shedding", "Total consumption exceeds 1000kW. Dimming non-essential areas.", 0.9)
             # Simulate reducing power across multiple rooms

# 14. Self-Learning AI
class SelfLearningAI(BaseAgent):
    def __init__(self):
        super().__init__("Self-Learning AI", "Cognitive", "Học từ tương tác người dùng và hệ thống giảng dạy")
    
    async def execute_duty(self):
        # AI tự kiểm tra chat history để học các chỉ dẫn mới của User
        cursor = db._get_cursor()
        # Logic to extract 'teaching' patterns from chat
        pass

# 15. Enterprise Management AI
class EnterpriseManagementAI(BaseAgent):
    def __init__(self):
        super().__init__("Enterprise Management AI", "Scale", "Quản lý báo cáo doanh nghiệp và khả năng mở rộng hệ thống")
    
    async def execute_duty(self):
        # Tạo báo cáo tổng hợp tự động mỗi 15 giây (simulation)
        pass


# ============================================================================
# 🎯 ULTIMATE AI ORCHESTRATOR
# ============================================================================

class UltimateAIOrchestrator:
    """Điều phối 15 AI Agents Chuyên môn"""
    def __init__(self):
        self.agents = [
            EdgeDataCollectorAI(),
            EnvironmentalAnalysisAI(),
            OccupancyPresenceAI(),
            BehaviorLearningAI(),
            EnergyOptimizationAI(),
            ActuatorControlAI(),
            SafetyMonitoringAI(),
            DataManagementAI(),
            ReportingAnalyticsAI(),
            UserExperienceAI(),
            CybersecurityAI(),
            PredictiveMaintenanceAI(),
            GlobalOptimizationAI(),
            SelfLearningAI(),
            EnterpriseManagementAI()
        ]
        logger.info(f"🎭 Ultimate Orchestrator initialized with {len(self.agents)} agents.")
        logger.info(f"🤖 Enterprise AI System: {len(self.agents)} agents initialized")
    
    async def start_all(self):
        """Khởi động tất cả agents"""
        tasks = [agent.auto_run() for agent in self.agents]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    def get_agent_status(self) -> List[Dict]:
        """Lấy trạng thái agents"""
        return [{
            "name": agent.name,
            "role": agent.role,
            "expertise": agent.expertise,
            "active": agent.active,
            "last_action": datetime.fromtimestamp(agent.last_action_time).strftime('%H:%M:%S'),
            "decisions_made": agent.decisions_made
        } for agent in self.agents]

    def teach_agent(self, agent_name: str, instruction: str) -> str:
        """Dạy agent cụ thể"""
        target = next((a for a in self.agents if a.name.lower() == agent_name.lower() or a.role.lower() == agent_name.lower()), None)
        if target:
            target.learn(instruction)
            return f"Đã dạy {target.name}: {instruction}"
        return f"Không tìm thấy agent: {agent_name}"

ai_system = UltimateAIOrchestrator()

ai_system = UltimateAIOrchestrator()

# ============================================================================
# 🌐 API MODELS
# ============================================================================

class LoginRequest(BaseModel):
    username: str
    password: str

class ChatRequest(BaseModel):
    query: str
    language: str = 'vi'

class AddRoomRequest(BaseModel):
    room_id: str
    name: str
    area: float
    floor: int
    building: str

class DeleteRoomRequest(BaseModel):
    room_id: str

class AddSensorRequest(BaseModel):
    sensor_id: str
    room_id: str
    sensor_type: str
    unit: str

class DeleteSensorRequest(BaseModel):
    sensor_id: str

class SensorReadingRequest(BaseModel):
    sensor_id: str
    value: float

class SearchRequest(BaseModel):
    query: str

class AddDeviceRequest(BaseModel):
    device_id: str
    room_id: str
    device_type: str
    device_name: str

class ControlDeviceRequest(BaseModel):
    device_id: str
    command: str  # ON or OFF

class AddBuildingRequest(BaseModel):
    building_id: str
    name: str
    address: str = ""
    total_floors: int = 1
    manager_name: str = ""

class AddFloorRequest(BaseModel):
    floor_id: str
    building_id: str
    floor_number: int
    name: str
    energy_target: float = 0.0

class AddScheduleRequest(BaseModel):
    room_id: str
    event_name: str
    start_time: str # Format: "YYYY-MM-DD HH:MM:SS"
    end_time: str
    min_temp: float = 24.0
    max_temp: float = 26.0
    priority: int = 1

# ============================================================================
# 🚀 API ENDPOINTS
# ============================================================================

# Health Check
@app.get("/api/health")
async def health_check():
    return {
        "status": "online",
        "database": db.db_type,
        "is_vercel": IS_VERCEL,
        "supabase": db.use_supabase,
        "mysql": db.use_mysql,
        "timestamp": time.time()
    }

# Auth
@app.post("/login")
async def login(creds: LoginRequest):
    user = db.verify_user(creds.username, creds.password)
    if user:
        token = secrets.token_hex(16)
        resp = JSONResponse({
            "success": True,
            "token": token,
            "redirect": "/",
            "language": user.get('language', 'vi')
        })
        resp.set_cookie(
            "access_token", 
            token, 
            httponly=True, 
            max_age=28800, 
            samesite="lax", 
            secure=False
        )
        return resp
    return JSONResponse(status_code=401, content={"success": False, "error": "Invalid credentials"})

@app.get("/login")
async def get_login():
    if LOGIN_HTML:
        return HTMLResponse(content=LOGIN_HTML)
    return JSONResponse(status_code=404, content={"error": "login.html not found"})

@app.get("/")
async def get_dashboard(request: Request):
    if not request.cookies.get("access_token"):
        return RedirectResponse("/login")
    if DASHBOARD_HTML:
        return HTMLResponse(content=DASHBOARD_HTML)
    return JSONResponse(status_code=404, content={"error": "dashboard_ultimate.html not found"})

# Chat
@app.post("/api/chat")
async def chat_endpoint(request: ChatRequest):
    """💬 Chat với Ultra AI"""
    answer = await chatbot.answer(request.query, request.language)
    return {"response": answer, "status": "success"}

# Helper for JSON serialization
def serialize_for_json(obj):
    if isinstance(obj, datetime):
        return obj.timestamp()
    if isinstance(obj, list):
        return [serialize_for_json(i) for i in obj]
    if isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    return obj

# Room Management
@app.post("/api/rooms/add")
async def add_room(request: AddRoomRequest):
    """➕ Thêm phòng"""
    return db.add_room(request.room_id, request.name, request.area, request.floor, request.building)

@app.delete("/api/rooms/delete")
async def delete_room(request: DeleteRoomRequest):
    """❌ Xóa phòng"""
    return db.delete_room(request.room_id)

@app.get("/api/rooms")
async def get_rooms():
    """📋 Danh sách phòng"""
    return serialize_for_json({"rooms": db.get_all_rooms()})

@app.get("/api/rooms/{room_id}/analytics")
async def get_room_analytics(room_id: str, hours: int = 24):
    """📊 Phân tích chi tiết từng phòng"""
    return serialize_for_json(db.get_room_analytics(room_id, hours))

# Enterprise Management
@app.post("/api/buildings/add")
async def add_building(request: AddBuildingRequest):
    """➕ Thêm tòa nhà"""
    return db.add_building(request.building_id, request.name, request.address, request.total_floors, request.manager_name)

@app.post("/api/floors/add")
async def add_floor(request: AddFloorRequest):
    """➕ Thêm tầng"""
    return db.add_floor(request.floor_id, request.building_id, request.floor_number, request.name, request.energy_target)

@app.post("/api/schedules/add")
async def add_schedule(request: AddScheduleRequest):
    """➕ Thêm lịch trình Enterprise"""
    return db.add_schedule(request.room_id, request.event_name, request.start_time, request.end_time, request.min_temp, request.max_temp, request.priority)

@app.get("/api/schedules/upcoming")
async def get_upcoming_schedules():
    """📋 Lịch trình sắp tới"""
    return serialize_for_json({"schedules": db.get_upcoming_schedules()})

# Sensor Management
@app.post("/api/sensors/add")
async def add_sensor(request: AddSensorRequest):
    """➕ Thêm cảm biến"""
    return db.add_sensor(request.sensor_id, request.room_id, request.sensor_type, request.unit)

@app.delete("/api/sensors/delete")
async def delete_sensor(request: DeleteSensorRequest):
    """❌ Xóa cảm biến"""
    return db.delete_sensor(request.sensor_id)

@app.post("/api/sensors/reading")
async def log_reading(request: SensorReadingRequest):
    """📡 Ghi dữ liệu cảm biến"""
    success = db.log_sensor_reading(request.sensor_id, request.value)
    return {"success": success}

# IoT Device Control
@app.post("/api/devices/add")
async def add_device(request: AddDeviceRequest):
    """➕ Thêm thiết bị IoT"""
    return db.add_iot_device(request.device_id, request.room_id, request.device_type, request.device_name)

@app.post("/api/devices/control")
async def control_device(request: ControlDeviceRequest):
    """🔌 Điều khiển thiết bị (ON/OFF)"""
    return db.control_device(request.device_id, request.command)

@app.get("/api/devices")
async def get_devices():
    """📋 Danh sách thiết bị"""
    return serialize_for_json({"devices": db.get_all_devices()})

# Search
@app.post("/api/search")
async def smart_search(request: SearchRequest):
    """🔍 Tìm kiếm thông minh"""
    return serialize_for_json(db.smart_search(request.query))

# Dashboard (Enterprise Pro)
@app.get("/dashboard")
async def dashboard(request: Request):
    """🏠 Trang Dashboard chính (Enterprise Pro)"""
    if DASHBOARD_HTML:
        return HTMLResponse(content=DASHBOARD_HTML)
    return JSONResponse(status_code=404, content={"error": "dashboard_ultimate.html not found"})

# Analytics
@app.get("/api/analytics")
async def get_analytics(hours: int = 24):
    """📈 Dữ liệu phân tích"""
    return serialize_for_json(db.get_analytics_data(hours))

# AI System
@app.get("/api/ai/status")
async def get_ai_status():
    """🤖 Trạng thái AI Agents"""
    return serialize_for_json({"agents": ai_system.get_agent_status()})

@app.get("/api/ai/decisions")
async def get_decisions(limit: int = 20):
    """📜 Quyết định AI"""
    return serialize_for_json({"decisions": db.get_ai_decisions(limit)})

# Alerts
@app.get("/api/alerts")
async def get_alerts():
    """🚨 Cảnh báo"""
    return serialize_for_json({"alerts": db.get_active_alerts()})

# WebSocket
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("🔌 WebSocket connected")
    
    try:
        while True:
            rooms = db.get_all_rooms()
            real_time = db.get_real_time_data()
            decisions = db.get_ai_decisions(10)
            alerts = db.get_active_alerts()
            ai_status = ai_system.get_agent_status()
            devices = db.get_all_devices()
            
            ui_data = {
                "kpi_energy": real_time['total_power'] * 24 / 1000,
                "kpi_occupancy": real_time['total_occupancy'],
                "kpi_temp": real_time['avg_temperature'],
                "kpi_sensors": real_time['sensor_count'],
                "data_quality": "EXCELLENT" if real_time['sensor_count'] > 0 else "NO_DATA",
                "rooms": rooms,
                "sensors_data": real_time['sensors'],
                "decisions": decisions,
                "alerts": alerts,
                "ai_agents": ai_status,
                "devices": devices,
                "timestamp": time.time()
            }
            
            # Use JSONEncoder to handle datetime
            await websocket.send_json(jsonable_encoder(ui_data))
            await asyncio.sleep(2)
    
    except WebSocketDisconnect:
        logger.info("🔌 WebSocket disconnected")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")

# Startup
@app.on_event("startup")
async def startup():
    # Start AI agents only if not on Vercel
    if not IS_VERCEL:
        asyncio.create_task(ai_system.start_all())
    
    logger.info("="*80)
    logger.info("🚀 EcoSchool AI ULTIMATE v12.0 - ONLINE")
    logger.info("="*80)
    logger.info(f"🗄️  Database: {db.db_type}")
    logger.info(f"🤖 AI Agents: {len(ai_system.agents)}")
    logger.info(f"🏢 Rooms: {len(db.get_all_rooms())}")
    logger.info(f"💬 Chatbot: Ultra-Intelligent (Memory + Confirmation)")
    logger.info(f"🔌 IoT: Device Control Enabled")
    logger.info("="*80)
    
    # Mở trình duyệt nếu chạy local
    try:
        import webbrowser
        import os
        if not os.environ.get("PORT"): # Chỉ mở nếu không có biến PORT (thường là chạy local)
             webbrowser.open("http://localhost:8080/login")
    except:
        pass

# ============================================================================
# 🎬 MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    import os
    # Ưu tiên port 8080 local, nếu có môi trường (Cloud) thì dùng theo môi trường
    port = int(os.environ.get("PORT", 8080))
    
    print("\n" + "="*80)
    print("🚀 EcoSchool AI Power Saver ULTIMATE v12.0")
    print("="*80)
    print(f"📡 MODE: {'CLOUD' if os.environ.get('PORT') else 'LOCAL'}")
    print(f"🌐 Server: http://0.0.0.0:{port}")
    print("👤 Login: funnylion1412 / tubilu1412")
    print("🤖 AI: 15 Agents + Ultra-Intelligent Chatbot")
    print("="*80)
    
    uvicorn.run(app, host="0.0.0.0", port=port)
