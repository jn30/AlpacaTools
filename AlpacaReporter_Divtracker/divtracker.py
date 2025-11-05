import sys
import os
import json
import traceback
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import requests
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
    QTableWidgetItem, QPushButton, QLabel, QLineEdit, QHeaderView,
    QMessageBox, QFileDialog, QComboBox, QInputDialog, QDialog,
    QCheckBox, QScrollArea
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor, QFont
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
)
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch, cm
from reportlab.lib import colors

STATE_FILE = "divtracker_state.json"
ALPACA_CONFIG = "alpaca_config.json"
SYNC_STATE_FILE = "alpaca_sync_state.json"
IGNORED_TRADES_FILE = "ignored_trades.json"
TRADES_CACHE_FILE = "trades_cache.json"

COLUMNS = [
    "KW", "Start", "Div/W", "Brutto", "WHT", "Netto", "DRIP", "Gesamt",
    "Preis", "Wert", "Ø/Woche", "Jahr", "Rendite", "Edit"
]
EDITABLE_COLS = [0, 2]
CALCULATED_COLS = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

def load_all_states():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_all_states(states):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(states, f, indent=2, ensure_ascii=False)

def get_last_viewed_etf():
    data = load_all_states()
    return data.get("_last_viewed_etf")

def save_last_viewed_etf(etf_name):
    data = load_all_states()
    data["_last_viewed_etf"] = etf_name
    save_all_states(data)

def load_alpaca_config():
    if os.path.exists(ALPACA_CONFIG):
        with open(ALPACA_CONFIG, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_alpaca_config(cfg):
    with open(ALPACA_CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)

def load_sync_state():
    if os.path.exists(SYNC_STATE_FILE):
        with open(SYNC_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_sync_state(state):
    with open(SYNC_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

def load_ignored_trades():
    if os.path.exists(IGNORED_TRADES_FILE):
        with open(IGNORED_TRADES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_ignored_trades(ignored):
    with open(IGNORED_TRADES_FILE, "w", encoding="utf-8") as f:
        json.dump(ignored, f, indent=2, ensure_ascii=False)

def load_trades_cache():
    if os.path.exists(TRADES_CACHE_FILE):
        with open(TRADES_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_trades_cache(cache):
    with open(TRADES_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def get_alpaca_positions(key, secret, use_paper=True):
    base = "https://paper-api.alpaca.markets" if use_paper else "https://api.alpaca.markets"
    url = f"{base}/v2/positions"
    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "accept": "application/json"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def get_activities_by_type(key, secret, activity_type, use_paper=True, after=None, until=None):
    base = "https://paper-api.alpaca.markets" if use_paper else "https://api.alpaca.markets"
    endpoint = f"{base}/v2/account/activities/{activity_type}"
    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "accept": "application/json"
    }

    all_activities = []
    page_count = 0
    
    params = {
        "direction": "asc",
        "page_size": "100"
    }
    
    if after:
        params["after"] = after
    if until:
        params["until"] = until
    
    while True:
        page_count += 1
        print(f"  [{activity_type}] Seite {page_count}...")
        
        try:
            r = requests.get(endpoint, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            if not data or len(data) == 0:
                print(f"  [{activity_type}] → Keine weiteren Daten")
                break
            
            all_activities.extend(data)
            print(f"  [{activity_type}] → {len(data)} Einträge ({len(all_activities)} gesamt)")
            
            if len(data) < 100:
                print(f"  [{activity_type}] → Letzte Seite erreicht")
                break
            
            last_date = data[-1].get("date") or data[-1].get("transaction_time")
            if last_date:
                params["after"] = last_date[:10]
            else:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"  [{activity_type}] → Fehler: {e}")
            break
    
    return all_activities

def get_all_activities_complete(key, secret, use_paper=True, after=None, until=None):
    print("\n" + "="*60)
    print("STARTE KOMPLETTE AKTIVITÄTS-ABFRAGE")
    print("="*60)
    
    all_activities = []
    
    print("\n1. Lade Trades (FILL)...")
    fills = get_activities_by_type(key, secret, "FILL", use_paper, after, until)
    all_activities.extend(fills)
    print(f"✓ {len(fills)} Trades geladen")
    
    print("\n2. Lade Dividenden (DIV)...")
    divs = get_activities_by_type(key, secret, "DIV", use_paper, after, until)
    all_activities.extend(divs)
    print(f"✓ {len(divs)} Dividenden geladen")
    
    print("\n3. Lade Quellensteuern (DIVNRA)...")
    divnra = get_activities_by_type(key, secret, "DIVNRA", use_paper, after, until)
    all_activities.extend(divnra)
    print(f"✓ {len(divnra)} Quellensteuern geladen")
    
    print(f"\n{'='*60}")
    print(f"GESAMT: {len(all_activities)} Aktivitäten")
    print(f"{'='*60}\n")
    
    return all_activities

class TradeEditorDialog(QDialog):
    def __init__(self, parent, sym, kw, trades_list):
        super().__init__(parent)
        self.sym = sym
        self.kw = kw
        self.trades_list = trades_list
        self.ignored_trades = load_ignored_trades()
        
        self.setWindowTitle(f"Trades bearbeiten - {sym} KW{kw}")
        self.setGeometry(200, 200, 600, 400)
        
        layout = QVBoxLayout()
        
        title = QLabel(f"<b>Trades für {sym} KW{kw}</b><br>Abhaken um zu ignorieren:")
        title.setFont(QFont("Arial", 11, QFont.Bold))
        layout.addWidget(title)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        content = QWidget()
        content_layout = QVBoxLayout(content)
        
        self.trade_checks = {}
        
        for i, trade in enumerate(trades_list):
            qty = trade.get("qty", 0)
            price = trade.get("price", 0)
            order_id = trade.get("order_id", f"{sym}_{kw}_{i}")
            
            is_ignored = self.ignored_trades.get(order_id, False)
            
            checkbox = QCheckBox(f"Ignorieren: {qty} Shares @ ${price}")
            checkbox.setChecked(is_ignored)
            
            content_layout.addWidget(checkbox)
            self.trade_checks[order_id] = (checkbox, trade)
        
        content_layout.addStretch()
        scroll.setWidget(content)
        layout.addWidget(scroll)
        
        buttons = QHBoxLayout()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Abbrechen")
        cancel_btn.clicked.connect(self.reject)
        buttons.addWidget(ok_btn)
        buttons.addWidget(cancel_btn)
        layout.addLayout(buttons)
        
        self.setLayout(layout)
    
    def accept(self):
        for order_id, (checkbox, trade) in self.trade_checks.items():
            if checkbox.isChecked():
                self.ignored_trades[order_id] = True
            else:
                self.ignored_trades.pop(order_id, None)
        
        save_ignored_trades(self.ignored_trades)
        super().accept()

class DivTracker(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DivTracker")
        self.setGeometry(100, 100, 1500, 700)

        states_raw = load_all_states()
        self.states = {k: v for k, v in states_raw.items() if not k.startswith("_")}
        if not self.states:
            self.states = {"ULTY": {"invest": 0.00, "rows": []}}

        last = get_last_viewed_etf()
        self.current_etf = last if last in self.states else list(self.states.keys())[0]

        self.block_signals = False
        self.trades_cache = load_trades_cache()
        self._build_gui()
        self.refresh_from_state()

    def _build_gui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        header = QHBoxLayout()
        header.addWidget(QLabel("ETF-Auswahl:"))
        
        self.etf_selector = QComboBox()
        self.etf_selector.addItems(sorted(self.states.keys()))
        self.etf_selector.setCurrentText(self.current_etf)
        self.etf_selector.setMinimumWidth(120)
        self.etf_selector.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.etf_selector.currentTextChanged.connect(self.on_etf_changed)
        header.addWidget(self.etf_selector)

        btn_add = QPushButton("+ ETF hinzufügen")
        btn_add.clicked.connect(self.add_etf)
        header.addWidget(btn_add)

        btn_del = QPushButton("ETF löschen")
        btn_del.clicked.connect(self.remove_etf)
        header.addWidget(btn_del)

        btn_sync = QPushButton("Alpaca Sync: Alle Tracker aktualisieren")
        btn_sync.clicked.connect(self.alpaca_sync_all)
        header.addWidget(btn_sync)

        btn_api = QPushButton("Alpaca API-Key")
        btn_api.clicked.connect(self.input_alpaca_api)
        header.addWidget(btn_api)

        header.addStretch()

        title_btn = QPushButton("DIVTRACKER")
        title_btn.setFont(QFont("Arial", 16, QFont.Bold))
        title_btn.setStyleSheet("background:#8FBC8F;color:white;padding:10px;")
        title_btn.clicked.connect(self.show_portfolio_overview)
        header.addWidget(title_btn)

        inv_label = QLabel("Invest:")
        inv_label.setFont(QFont("Arial", 10, QFont.Bold))
        inv_label.setStyleSheet("background:#8FBC8F;color:white;padding:10px;")
        header.addWidget(inv_label)
        
        self.invest_input = QLineEdit("0.00")
        self.invest_input.setMaximumWidth(150)
        self.invest_input.setStyleSheet("padding:5px;font-size:12pt;")
        self.invest_input.textChanged.connect(self.on_data_changed)
        header.addWidget(self.invest_input)
        
        usd_label = QLabel("USD")
        usd_label.setStyleSheet("background:#8FBC8F;color:white;padding:10px;")
        header.addWidget(usd_label)

        layout.addLayout(header)

        buttons = QHBoxLayout()
        btn_week = QPushButton("+ Woche hinzufügen")
        btn_week.clicked.connect(self.add_week)
        btn_week.setStyleSheet("background:#28a745;color:white;padding:10px;font-weight:bold;")
        buttons.addWidget(btn_week)

        btn_csv = QPushButton("📊 Als CSV exportieren")
        btn_csv.clicked.connect(self.export_csv)
        btn_csv.setStyleSheet("background:#28a745;color:white;padding:10px;font-weight:bold;")
        buttons.addWidget(btn_csv)

        btn_pdf = QPushButton("📄 Als PDF exportieren")
        btn_pdf.clicked.connect(self.export_pdf)
        btn_pdf.setStyleSheet("background:#007bff;color:white;padding:10px;font-weight:bold;")
        buttons.addWidget(btn_pdf)

        btn_reset = QPushButton("🔄 Zurücksetzen")
        btn_reset.clicked.connect(self.reset)
        btn_reset.setStyleSheet("background:#dc3545;color:white;padding:10px;font-weight:bold;")
        buttons.addWidget(btn_reset)
        buttons.addStretch()
        layout.addLayout(buttons)

        self.table = QTableWidget()
        self.table.setColumnCount(len(COLUMNS))
        self.table.setHorizontalHeaderLabels(COLUMNS)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(
            "QTableWidget{gridline-color:#ccc;background:white;}"
            "QHeaderView::section{background:#8FBC8F;color:white;padding:10px;font-weight:bold;}"
        )
        self.table.itemChanged.connect(self.on_cell_changed)
        layout.addWidget(self.table)

        self.status_label = QLabel("Status: Bereit")
        self.statusBar().addPermanentWidget(self.status_label)

    def add_etf(self):
        name, ok = QInputDialog.getText(self, "ETF hinzufügen", "Name/Ticker des neuen ETFs:")
        if ok and name.strip():
            if name in self.states:
                QMessageBox.warning(self, "Fehler", "Dieser Name existiert bereits.")
                return
            self.states[name] = {"invest": 0.00, "rows": []}
            self.etf_selector.clear()
            self.etf_selector.addItems(sorted(self.states.keys()))
            self.etf_selector.setCurrentText(name)
            save_all_states(self.states)

    def remove_etf(self):
        name = self.etf_selector.currentText()
        if len(self.states) == 1:
            QMessageBox.warning(self, "Hinweis", "Mindestens ein ETF muss bestehen bleiben.")
            return
        if QMessageBox.question(self, "Löschen", f"'{name}' wirklich löschen?",
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            self.states.pop(name, None)
            self.etf_selector.clear()
            self.etf_selector.addItems(sorted(self.states.keys()))
            save_all_states(self.states)
            self.current_etf = self.etf_selector.currentText()
            self.refresh_from_state()

    def on_etf_changed(self, etf):
        if not etf:
            return
        self.current_etf = etf
        save_last_viewed_etf(etf)
        self.refresh_from_state()
        self.status_label.setText(f"Status: ETF '{etf}' geladen")

    def refresh_from_state(self):
        self.block_signals = True
        st = self.states.get(self.current_etf, {"invest": 0.00, "rows": []})
        self.invest_input.setText(f"{st.get('invest', 0.00):.2f}")
        rows = st.get("rows", [])
        if rows:
            self.populate_table(rows)
        else:
            self.create_empty_row()
        self.block_signals = False
        self.recalculate_all()

    def create_empty_row(self):
        self.table.setRowCount(1)
        for c in range(len(COLUMNS)):
            if c == len(COLUMNS) - 1:
                btn = QPushButton("Edit")
                btn.clicked.connect(lambda checked, r=0: self.open_trade_editor(r))
                self.table.setCellWidget(0, c, btn)
            else:
                itm = QTableWidgetItem("")
                if c in CALCULATED_COLS:
                    itm.setFlags(Qt.ItemIsEnabled)
                    itm.setBackground(QColor(230, 240, 250))
                else:
                    itm.setFlags(Qt.ItemIsEnabled | Qt.ItemIsEditable | Qt.ItemIsSelectable)
                self.table.setItem(0, c, itm)

    def populate_table(self, rows):
        self.table.setRowCount(len(rows))
        for r, data in enumerate(rows):
            for c, name in enumerate(COLUMNS):
                if c == len(COLUMNS) - 1:
                    btn = QPushButton("Edit")
                    btn.clicked.connect(lambda checked, row=r: self.open_trade_editor(row))
                    self.table.setCellWidget(r, c, btn)
                else:
                    val = data.get(name, "")
                    itm = QTableWidgetItem(str(val))
                    if c in CALCULATED_COLS:
                        itm.setFlags(Qt.ItemIsEnabled)
                        itm.setBackground(QColor(230, 240, 250))
                    else:
                        itm.setFlags(Qt.ItemIsEnabled | Qt.ItemIsEditable | Qt.ItemIsSelectable)
                    self.table.setItem(r, c, itm)

    def open_trade_editor(self, row):
        kw = self.table.item(row, 0).text() if self.table.item(row, 0) else None
        if not kw or self.current_etf not in self.trades_cache or kw not in self.trades_cache[self.current_etf]:
            QMessageBox.information(self, "Info", "Keine Trades für diese Woche verfügbar.")
            return
        
        trades = self.trades_cache[self.current_etf][kw]
        dialog = TradeEditorDialog(self, self.current_etf, kw, trades)
        if dialog.exec_() == QDialog.Accepted:
            self.alpaca_sync_all()

    def on_cell_changed(self, item):
        if self.block_signals:
            return
        if item.column() in EDITABLE_COLS:
            self.recalculate_all()
            self.save_state()

    def on_data_changed(self):
        if not self.block_signals:
            self.recalculate_all()
            self.save_state()

    def recalculate_all(self):
        self.block_signals = True
        try:
            ini = float(self.invest_input.text().replace(",", "") or 0)
        except ValueError:
            ini = 0.0
        
        total_netto_dividends = 0.0
        
        for r in range(self.table.rowCount()):
            try:
                price = float(self.table.item(r, 8).text().replace(",", "").replace("$", "").strip() or 0)
            except (AttributeError, ValueError):
                price = 0.0
            
            try:
                netto = float(self.table.item(r, 5).text().replace(",", "").replace("$", "").strip() or 0)
            except (AttributeError, ValueError):
                netto = 0.0
            
            drip = int(netto / price) if price and netto else 0
            
            try:
                total_shares = int(self.table.item(r, 7).text().replace(",", "").strip() or 0)
            except (AttributeError, ValueError):
                total_shares = 0
            
            value = total_shares * price
            total_netto_dividends += netto
            avg_week = total_netto_dividends / (r + 1) if (r + 1) > 0 else 0
            annual = avg_week * 52
            total_return = total_netto_dividends - ini
            
            self.table.item(r, 6).setText(str(drip))
            self.table.item(r, 9).setText(f"${value:,.2f}")
            self.table.item(r, 10).setText(f"${avg_week:,.2f}")
            self.table.item(r, 11).setText(f"${annual:,.2f}")
            
            rend = self.table.item(r, 12)
            rend.setText(f"${total_return:,.2f}")
            rend.setForeground(QColor(0, 128, 0) if total_return >= 0 else QColor(255, 0, 0))
        
        self.block_signals = False

    def input_alpaca_api(self):
        cfg = load_alpaca_config()
        key, ok = QInputDialog.getText(self, "API Key", "Alpaca API Key:", text=cfg.get("key", ""))
        if not ok:
            return
        secret, ok = QInputDialog.getText(self, "API Secret", "Alpaca Secret:", text=cfg.get("secret", ""))
        if not ok:
            return
        mode, ok = QInputDialog.getItem(self, "Modus", "API-Modus auswählen:", 
                                        ["Live", "Paper"], 0, False)
        if ok:
            cfg.update({"key": key, "secret": secret, "mode": mode.lower()})
            save_alpaca_config(cfg)
            QMessageBox.information(self, "Gespeichert", f"API-Daten ({mode}) erfolgreich gespeichert")

    def alpaca_sync_all(self):
        cfg = load_alpaca_config()
        key, secret, mode = cfg.get("key"), cfg.get("secret"), cfg.get("mode", "live")
        if not key or not secret:
            QMessageBox.warning(self, "Fehler", "Bitte zuerst Alpaca-API-Daten eingeben")
            return
        
        use_paper = mode == "paper"
        today = datetime.today()
        tomorrow = today + timedelta(days=1)
        until = tomorrow.strftime("%Y-%m-%d")
        current_year, current_week, _ = today.isocalendar()
        current_kw = f"{current_week:02d}/{current_year}"

        # Remember current ETF BEFORE sync
        saved_etf = self.current_etf

        try:
            acts = get_all_activities_complete(key, secret, use_paper, after=None, until=until)
            
            positions = get_alpaca_positions(key, secret, use_paper)
            print(f"✓ {len(positions)} Positionen abgerufen\n")

            symbols_with_divs = set()
            for a in acts:
                if a.get("activity_type") in ["DIV", "DIVNRA"]:
                    sym = a.get("symbol")
                    if sym:
                        symbols_with_divs.add(sym)
            
            print(f"✓ {len(symbols_with_divs)} Symbole mit Dividenden gefunden\n")

            for sym in list(self.states.keys()):
                if sym not in symbols_with_divs:
                    print(f"⊗ {sym}: Keine Dividenden → wird entfernt")
                    self.states.pop(sym, None)

            for sym in symbols_with_divs:
                if sym not in self.states:
                    self.states[sym] = {"invest": 0.00, "rows": []}

            grouped = defaultdict(lambda: defaultdict(lambda: {
                "div_gross": 0.0,
                "div_tax": 0.0,
                "buy_total_cost": 0.0,
                "buy_total_qty": 0.0,
                "sell_total_cost": 0.0,
                "sell_total_qty": 0.0,
                "trades": []
            }))
            
            ignored_trades = load_ignored_trades()
            
            # Track seen order_ids to prevent duplicates
            seen_order_ids = set()

            for a in acts:
                sym = a.get("symbol")
                if not sym or sym not in self.states:
                    continue

                typ = a.get("activity_type", "")
                dstr = a.get("date") or a.get("transaction_time", "")[:10]
                if not dstr:
                    continue

                dt = datetime.fromisoformat(dstr)
                y, w, _ = dt.isocalendar()
                kw = f"{w:02d}/{y}"

                if typ == "FILL":
                    side = a.get("side")
                    trade_type = a.get("type", "").lower().strip()
                    if trade_type not in ["fill", "partial_fill"]:
                        continue

                    order_id = a.get("id", f"{sym}_{kw}")

                    if order_id in seen_order_ids:
                        print(f"  [DUPLICATE] {order_id} - übersprungen")
                        continue
                    
                    seen_order_ids.add(order_id)

                    if ignored_trades.get(order_id, False):
                        print(f"  [IGNORED] {order_id}")
                        continue

                    qty = float(a.get("qty", 0))
                    if qty <= 0:
                        continue

                    price = float(a.get("price", 0))
                    cost = qty * price

                    if side == "buy":
                        grouped[sym][kw]["buy_total_cost"] += cost
                        grouped[sym][kw]["buy_total_qty"] += qty
                        grouped[sym][kw]["trades"].append({
                            "order_id": order_id,
                            "qty": qty,
                            "price": price
                        })
                    elif side == "sell":
                        grouped[sym][kw]["sell_total_cost"] += cost
                        grouped[sym][kw]["sell_total_qty"] += qty

                    
                elif typ == "DIV":
                    gross_amt = float(a.get("net_amount", a.get("amount", 0)))
                    grouped[sym][kw]["div_gross"] += gross_amt
                    
                elif typ == "DIVNRA":
                    tax_amt = float(a.get("net_amount", a.get("amount", 0)))
                    grouped[sym][kw]["div_tax"] += tax_amt

            if self.current_etf not in self.trades_cache:
                self.trades_cache[self.current_etf] = {}

            for sym, weeks in grouped.items():
                print(f"\n{'='*60}")
                print(f"Verarbeite {sym}: {len(weeks)} Wochen")
                print(f"{'='*60}")
                
                rows = []
                current_price = 0.0
                
                for pos in positions:
                    if pos["symbol"] == sym:
                        current_price = float(pos["current_price"])
                        break
                
                if current_kw not in weeks:
                    weeks[current_kw] = {
                        "div_gross": 0.0,
                        "div_tax": 0.0,
                        "buy_total_cost": 0.0,
                        "buy_total_qty": 0.0,
                        "sell_total_cost": 0.0,
                        "sell_total_qty": 0.0,
                        "trades": []
                    }
                
                sorted_weeks = sorted(weeks.keys(), 
                                    key=lambda k: (int(k.split("/")[1]), int(k.split("/")[0])))
                
                # Load existing rows for Div/W protection
                existing_rows = {r["KW"]: r for r in self.states.get(sym, {}).get("rows", [])}
                
                
                print(f"Wochen: {', '.join(sorted_weeks)}")
                print(f"Aktuelle Woche: {current_kw}\n")
                
                total_invested = 0.0
                cumulative_shares = 0
                cumulative_div_total = 0.0
                week_count = 0
                
                for kw in sorted_weeks:
                    vals = weeks[kw]
                    row = {c: "" for c in COLUMNS}
                    row["KW"] = kw
                    
                    if sym not in self.trades_cache:
                        self.trades_cache[sym] = {}
                    self.trades_cache[sym][kw] = vals["trades"]
                    
                    start_shares_this_week = cumulative_shares
                    row["Start"] = str(start_shares_this_week)
                    
                    gross_div = vals["div_gross"]
                    wht_div = abs(vals["div_tax"])
                    net_div = gross_div - wht_div
                    buy_cost = vals["buy_total_cost"]
                    buy_qty = vals["buy_total_qty"]
                    sell_cost = vals["sell_total_cost"]
                    sell_qty = vals["sell_total_qty"]
                    
                    row["Brutto"] = f"${gross_div:,.2f}"
                    row["WHT"] = f"${wht_div:,.2f}"
                    row["Netto"] = f"${net_div:,.2f}"
                    
                    print(f"{kw}: DIV=${gross_div:.2f}, DIVNRA=${wht_div:.2f}, Netto=${net_div:.2f}")
                    
                    # Div/W PROTECTION: Check if manually set
                    if kw in existing_rows and existing_rows[kw].get("Div/W", "").strip():
                        row["Div/W"] = existing_rows[kw]["Div/W"]
                        print(f"{kw}: Div/W manuell gesetzt → behalten: {row['Div/W']}")
                    else:
                        if start_shares_this_week > 0:
                            rate_per_week = gross_div / start_shares_this_week
                            row["Div/W"] = f"{rate_per_week:.4f}"
                            print(f"{kw}: Div/W = ${gross_div:.2f} / {start_shares_this_week} = {rate_per_week:.4f}")
                        else:
                            row["Div/W"] = ""
                    
                    if buy_qty > 0:
                        cumulative_shares += int(buy_qty)
                        avg_buy_price = buy_cost / buy_qty
                        row["Preis"] = f"{avg_buy_price:.2f}"
                        print(f"{kw}: Kauf +{int(buy_qty)} @ ${avg_buy_price:.2f} → {cumulative_shares}")
                        
                        if net_div >= buy_cost:
                            drip_shares = int(net_div / avg_buy_price)
                            print(f"{kw}: DRIP erkannt! ${net_div:.2f} Div >= ${buy_cost:.2f} Kauf")
                            row["DRIP"] = str(drip_shares)
                        else:
                            net_investment = buy_cost - net_div
                            total_invested += net_investment
                            print(f"{kw}: Invest +${net_investment:.2f} (Käufe: ${buy_cost:.2f}, Div: ${net_div:.2f})")
                            row["DRIP"] = "0"
                    else:
                        if current_price > 0:
                            row["Preis"] = f"{current_price:.2f}"
                        row["DRIP"] = "0"

                    if sell_qty > 0:
                        total_invested -= sell_cost
                        cumulative_shares = 0
                        print(f"{kw}: Verkauf von {int(sell_qty)} Aktien. Bestand auf 0 gesetzt.")
                        print(f"{kw}: Investition um ${sell_cost:.2f} reduziert.")
                    
                    row["Gesamt"] = str(cumulative_shares)

                    # Calculate value
                    if current_price > 0:
                        wert = cumulative_shares * current_price
                        row["Wert"] = f"${wert:,.2f}"
                    else:
                        row["Wert"] = "$0.00"

                    # Cumulative dividends
                    cumulative_div_total += net_div
                    week_count += 1

                    # Ø/Woche
                    if week_count > 0:
                        avg_per_week = cumulative_div_total / week_count
                        row["Ø/Woche"] = f"${avg_per_week:.2f}"
                    else:
                        row["Ø/Woche"] = "$0.00"

                    # Jahr (52 weeks projected)
                    if week_count > 0:
                        yearly = (cumulative_div_total / week_count) * 52
                        row["Jahr"] = f"${yearly:,.2f}"
                    else:
                        row["Jahr"] = "$0.00"

                    # Rendite
                    rendite = cumulative_div_total - total_invested
                    if rendite < 0:
                        row["Rendite"] = f"$-{abs(rendite):,.2f}"
                    else:
                        row["Rendite"] = f"${rendite:,.2f}"

                    row["Edit"] = "Edit"

                    rows.append(row)
                
                print(f"\n✓ {len(rows)} Zeilen, ${total_invested:.2f}, {cumulative_shares} Shares\n")
                
                self.states[sym]["invest"] = round(total_invested, 2)
                self.states[sym]["rows"] = rows

            save_sync_state({"last_activity_after": until})
            save_all_states(self.states)
            save_trades_cache(self.trades_cache)
            
            # AFTER SYNC: Restore ETF dropdown
            self.etf_selector.blockSignals(True)
            self.etf_selector.clear()
            self.etf_selector.addItems(sorted(self.states.keys()))
            
            # Try to select saved ETF
            if saved_etf in self.states:
                self.etf_selector.setCurrentText(saved_etf)
                self.current_etf = saved_etf
            else:
                # If ETF no longer exists, select first one
                if self.states:
                    first_etf = list(self.states.keys())[0]
                    self.etf_selector.setCurrentText(first_etf)
                    self.current_etf = first_etf
            
            self.etf_selector.blockSignals(False)
            self.refresh_from_state()
            
            QMessageBox.information(self, "Sync erfolgreich", 
                f"✓ {len(acts)} Aktivitäten synchronisiert\n"
                f"✓ {len(self.states)} ETFs aktualisiert")
            
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"{str(e)}\n\n{traceback.format_exc()}")

    def add_week(self):
        self.block_signals = True
        r = self.table.rowCount()
        self.table.insertRow(r)
        for c in range(len(COLUMNS)):
            if c == len(COLUMNS) - 1:
                btn = QPushButton("Edit")
                btn.clicked.connect(lambda checked, row=r: self.open_trade_editor(row))
                self.table.setCellWidget(r, c, btn)
            else:
                itm = QTableWidgetItem("")
                if c in CALCULATED_COLS:
                    itm.setFlags(Qt.ItemIsEnabled)
                    itm.setBackground(QColor(230, 240, 250))
                else:
                    itm.setFlags(Qt.ItemIsEnabled | Qt.ItemIsEditable | Qt.ItemIsSelectable)
                self.table.setItem(r, c, itm)
        self.block_signals = False
        self.recalculate_all()
        self.save_state()

    def save_state(self):
        try:
            ini = float(self.invest_input.text().replace(",", "") or 0)
        except ValueError:
            ini = 0.0
        rows = []
        for r in range(self.table.rowCount()):
            data = {}
            for c, name in enumerate(COLUMNS[:-1]):
                itm = self.table.item(r, c)
                data[name] = itm.text() if itm else ""
            rows.append(data)
        self.states[self.current_etf] = {
            "invest": round(ini, 2),
            "rows": rows,
            "last_modified": datetime.now().isoformat()
        }
        save_all_states(self.states)

    def export_csv(self):
        fname, _ = QFileDialog.getSaveFileName(
            self, "CSV exportieren",
            f"{self.current_etf}_divtracker-{datetime.now():%Y-%m-%d}.csv",
            "CSV Files (*.csv)"
        )
        if fname:
            data = [[self.table.item(r, c).text() if self.table.item(r, c) else ""
                     for c in range(len(COLUMNS)-1)] for r in range(self.table.rowCount())]
            pd.DataFrame(data, columns=COLUMNS[:-1]).to_csv(fname, index=False, encoding="utf-8-sig")
            QMessageBox.information(self, "Erfolg", f"CSV exportiert:\n{fname}")

    def export_pdf(self):
        fname, _ = QFileDialog.getSaveFileName(
            self, "PDF exportieren",
            f"{self.current_etf}_divtracker-{datetime.now():%Y-%m-%d}.pdf",
            "PDF Files (*.pdf)"
        )
        if not fname:
            return
        doc = SimpleDocTemplate(fname, pagesize=landscape(A4))
        styles = getSampleStyleSheet()
        elems = [Paragraph(f"DIVTRACKER – {self.current_etf} – {datetime.now():%d.%m.%Y}",
                          styles["Title"]), Spacer(1, 0.3*inch)]
        data = [COLUMNS[:-1]]
        for r in range(self.table.rowCount()):
            data.append([self.table.item(r, c).text() if self.table.item(r, c) else ""
                        for c in range(len(COLUMNS)-1)])
        widths = [1.2*cm,1.5*cm,2*cm,1.7*cm,1.2*cm,1.5*cm,1.3*cm,1.7*cm,
                  1.8*cm,1.8*cm,1.2*cm,1.5*cm,2*cm]
        tbl = Table(data, repeatRows=1, colWidths=widths)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#8FBC8F")),
            ("TEXTCOLOR", (0,0), (-1,0), colors.whitesmoke),
            ("ALIGN", (0,0), (-1,-1), "CENTER"),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE", (0,0), (-1,-1), 8),
            ("GRID", (0,0), (-1,-1), 0.5, colors.black),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#D6E4F5")])
        ]))
        elems.append(tbl)
        doc.build(elems)
        QMessageBox.information(self, "Erfolg", f"PDF exportiert:\n{fname}")

    def reset(self):
        if QMessageBox.question(self, "Zurücksetzen",
            f"'{self.current_etf}' wirklich löschen?",
            QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            self.invest_input.setText("0.00")
            self.create_empty_row()
            self.save_state()

    def show_portfolio_overview(self):
        """Display portfolio overview with total investment and returns"""
        gesamtinvest = 0.0
        gesamtwert = 0.0
        rueckzahlung_invest = 0.0
        
        for sym in self.states:
            gesamtinvest += self.states[sym]["invest"]
            if self.states[sym]["rows"]:
                try:
                    # Get current market value from last row
                    wert_str = self.states[sym]["rows"][-1].get("Wert", "$0.00").replace(",", "").replace("$", "")
                    gesamtwert += float(wert_str)
                    
                    # Get "Rendite" (return from dividends) from last row
                    rendite_str = self.states[sym]["rows"][-1].get("Rendite", "$0.00").replace(",", "").replace("$", "")
                    rueckzahlung_invest += float(rendite_str)
                except Exception:
                    pass
        
        # Total return = Market value - Investment
        gesamtrendite = gesamtwert - gesamtinvest
        
        msg = (
            f"Portfolioübersicht\n\n"
            f"Gesamtinvestition aller ETFs: ${gesamtinvest:,.2f}\n"
            f"Aktueller Marktwert: ${gesamtwert:,.2f}\n"
            f"Gesamtrendite (Wert - Invest): ${gesamtrendite:,.2f}\n"
            f"Rückzahlung Invest (Dividenden): ${rueckzahlung_invest:,.2f}"
        )
        QMessageBox.information(self, "Portfolio Übersicht", msg)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    tracker = DivTracker()
    tracker.show()
    sys.exit(app.exec_())