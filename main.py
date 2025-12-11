import customtkinter as ctk
import pandas as pd
from sqlalchemy import create_engine, inspect
import pyodbc
import threading
import json
import os
import sys
import base64
from datetime import datetime
from CTkMessagebox import CTkMessagebox
import warnings

# --- AYARLAR ---
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

# Kılavuz ve Künye Metni (BURAYI KENDİNE GÖRE DÜZENLE)
HELP_TEXT = """
TGH DATA INTEGRATOR - KULLANIM KILAVUZU

1. VERİTABANI BAĞLANTISI
   • Sol paneldeki kutucuklara PostgreSQL bağlantı bilgilerinizi girin.
   • 'Hedef Tablo' kısmına verilerin aktarılacağı SQL tablosunun adını yazın.

2. DOSYA VE TABLO SEÇİMİ
   • Sağ paneldeki '📁' butonuna basarak Excel veya Access dosyanızı seçin.
   • Dosya seçildikten sonra hemen altındaki listeden aktarmak istediğiniz
     Tabloyu (Access) veya Sayfayı (Excel) seçin.

3. ANALİZ (X-RAY)
   • 'BAĞLANTIYI TEST ET & ANALİZ ET' butonuna basın.
   • Program, veritabanı ile dosyanızı karşılaştıracaktır.
   • 'Şema Röntgeni' sekmesinden hangi sütunların eşleştiğini, hangilerinin
     yeni oluşturulacağını inceleyin.

4. AKTARIM BAŞLATMA
   • Analiz sorunsuzsa en alttaki 'BAŞLAT' butonu aktif olacaktır.
   • Butona basarak aktarımı başlatın. İlerleme çubuğundan takibini yapın.

5. GÜVENLİK VE AYARLAR
   • Girdiğiniz şifreler şifrelenerek (Encrypted) kaydedilir.
   • Bir sonraki açılışta ayarlarınız otomatik yüklenir.

---------------------------------------------------------
GELİŞTİRİCİ BİLGİLERİ

Bu yazılım TÜRKİYE HASTANESİ Dijital Dönüşüm Ekibi tarafından geliştirilmiştir.
Amaç, kurumsal veri aktarım süreçlerini güvenli ve hızlı hale getirmektir.

Mimar & Geliştirici: TÜRKİYE HASTANESİ Dijital Dönüşüm Ekibi(İhsan ARVAS)
Versiyon: 5.1 (Stable)
Altyapı: Python, SQLAlchemy, CustomTkinter
Dahili: 1593
"""


def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


class UltimateETLApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("TGH Data Integrator v5.1")
        self.geometry("1000x850")

        try:
            self.iconbitmap(resource_path("logo.ico"))
        except:
            pass

        self.config_file = "etl_secure_config.json"
        self.stop_event = threading.Event()

        self.init_ui()
        self.load_settings()

    def init_ui(self):
        # HEADER
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.grid(row=0, column=0, sticky="ew", padx=20, pady=(20, 10))
        ctk.CTkLabel(self.header_frame, text="TÜRKİYE HASTANESİ Data Integrator", font=("Helvetica", 26, "bold"),
                     text_color="#3B8ED0").pack(side="left")
        ctk.CTkLabel(self.header_frame, text="| THDX Edition", font=("Helvetica", 16), text_color="gray").pack(
            side="left", padx=10, pady=(8, 0))

        # CONFIG PANEL
        self.config_frame = ctk.CTkFrame(self)
        self.config_frame.grid(row=1, column=0, sticky="ew", padx=20, pady=10)

        # -- Sol: DB Ayarları --
        self.lbl_db = ctk.CTkLabel(self.config_frame, text="Veritabanı (PostgreSQL)", font=("Roboto", 14, "bold"))
        self.lbl_db.grid(row=0, column=0, padx=15, pady=10, sticky="w")

        self.entry_host = ctk.CTkEntry(self.config_frame, placeholder_text="Host IP", width=140)
        self.entry_host.grid(row=1, column=0, padx=10, pady=5)
        self.entry_db = ctk.CTkEntry(self.config_frame, placeholder_text="DB Name", width=140)
        self.entry_db.grid(row=1, column=1, padx=10, pady=5)
        self.entry_user = ctk.CTkEntry(self.config_frame, placeholder_text="User", width=140)
        self.entry_user.grid(row=2, column=0, padx=10, pady=5)
        self.entry_pass = ctk.CTkEntry(self.config_frame, placeholder_text="Password", show="*", width=140)
        self.entry_pass.grid(row=2, column=1, padx=10, pady=5)

        self.entry_target_table = ctk.CTkEntry(self.config_frame, placeholder_text="Hedef Tablo (SQL)", width=290)
        self.entry_target_table.grid(row=3, column=0, columnspan=2, padx=10, pady=(5, 15))

        # -- Sağ: Dosya Ayarları --
        self.lbl_file = ctk.CTkLabel(self.config_frame, text="Kaynak Dosya", font=("Roboto", 14, "bold"))
        self.lbl_file.grid(row=0, column=2, padx=15, pady=10, sticky="w")

        self.entry_file_path = ctk.CTkEntry(self.config_frame, placeholder_text="Dosya Yolu...", width=300)
        self.entry_file_path.grid(row=1, column=2, columnspan=2, padx=10, pady=5)

        self.btn_browse = ctk.CTkButton(self.config_frame, text="📁", width=40, command=self.select_file,
                                        fg_color="#555")
        self.btn_browse.grid(row=1, column=4, padx=5, pady=5)

        self.option_source_name = ctk.CTkComboBox(self.config_frame, width=300, values=["Önce Dosya Seçin..."])
        self.option_source_name.grid(row=2, column=2, columnspan=2, padx=10, pady=5)

        # Analiz Butonu
        self.btn_analyze = ctk.CTkButton(self.config_frame, text="🔍 BAĞLANTIYI TEST ET & ANALİZ ET",
                                         command=self.start_analysis_thread,
                                         fg_color="#E07A5F", hover_color="#C0583F",
                                         width=300, height=40, font=("Roboto", 13, "bold"))
        self.btn_analyze.grid(row=3, column=2, columnspan=3, padx=10, pady=(10, 15))

        # --- PROGRESS BAR ---
        self.progress_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.progress_frame.grid(row=2, column=0, sticky="ew", padx=20, pady=(0, 5))

        self.lbl_progress = ctk.CTkLabel(self.progress_frame, text="Sistem Hazır", font=("Roboto", 12))
        self.lbl_progress.pack(anchor="w")

        self.progressbar = ctk.CTkProgressBar(self.progress_frame, orientation="horizontal", height=15)
        self.progressbar.pack(fill="x", pady=5)
        self.progressbar.set(0)

        # --- TABS (YENİ SEKME EKLENDİ) ---
        self.tab_view = ctk.CTkTabview(self)
        self.tab_view.grid(row=3, column=0, sticky="nsew", padx=20, pady=5)

        self.tab_mapping = self.tab_view.add("Şema Röntgeni (X-Ray)")
        self.tab_logs = self.tab_view.add("Canlı Loglar")
        self.tab_help = self.tab_view.add("Nasıl Kullanılır? & Künye")  # <--- YENİ SEKME

        # Mapping Tab İçeriği
        self.scroll_mapping = ctk.CTkScrollableFrame(self.tab_mapping, label_text="Sütun Eşleşme Analizi")
        self.scroll_mapping.pack(fill="both", expand=True, padx=5, pady=5)
        self.lbl_mapping_status = ctk.CTkLabel(self.tab_mapping, text="Analiz bekleniyor...", text_color="gray")
        self.lbl_mapping_status.pack(pady=5)

        # Log Tab İçeriği
        self.textbox_log = ctk.CTkTextbox(self.tab_logs, state="disabled", font=("Consolas", 12))
        self.textbox_log.pack(fill="both", expand=True, padx=5, pady=5)

        # --- YARDIM SEKME İÇERİĞİ (YENİ) ---
        self.textbox_help = ctk.CTkTextbox(self.tab_help, font=("Roboto", 13))
        self.textbox_help.pack(fill="both", expand=True, padx=5, pady=5)
        self.textbox_help.insert("0.0", HELP_TEXT)  # Metni içeri bas
        self.textbox_help.configure(state="disabled")  # Salt okunur yap

        # FOOTER
        self.footer_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.footer_frame.grid(row=4, column=0, sticky="ew", padx=20, pady=20)

        self.btn_run = ctk.CTkButton(self.footer_frame, text="🚀 BAŞLAT",
                                     command=self.start_transfer_thread, state="disabled",
                                     fg_color="#2A9D8F", hover_color="#21867A", height=50, font=("Roboto", 16, "bold"))
        self.btn_run.pack(side="left", fill="x", expand=True, padx=(0, 10))

        self.btn_stop = ctk.CTkButton(self.footer_frame, text="⛔ DURDUR",
                                      command=self.stop_process, state="disabled",
                                      fg_color="#D62828", hover_color="#A01D1D", height=50, width=150,
                                      font=("Roboto", 16, "bold"))
        self.btn_stop.pack(side="right")

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

    # --- ŞİFRELEME ---
    def encrypt(self, text):
        return base64.b64encode(text.encode()).decode()

    def decrypt(self, encoded_text):
        try:
            return base64.b64decode(encoded_text.encode()).decode()
        except:
            return ""

    # --- DOSYA İŞLEMLERİ ---
    def select_file(self):
        f = ctk.filedialog.askopenfilename(filetypes=[("Veri Dosyaları", "*.xlsx *.xls *.mdb *.accdb")])
        if f:
            self.entry_file_path.delete(0, "end")
            self.entry_file_path.insert(0, f)
            threading.Thread(target=self.populate_tables, args=(f,), daemon=True).start()

    def populate_tables(self, file_path):
        self.option_source_name.set("Aranıyor...")
        self.option_source_name.configure(values=[])
        tables = []
        try:
            if file_path.endswith(('.xlsx', '.xls')):
                xl = pd.ExcelFile(file_path)
                tables = xl.sheet_names
            elif file_path.endswith(('.mdb', '.accdb')):
                conn_str = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" rf"DBQ={file_path};")
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                tables = [x.table_name for x in cursor.tables(tableType='TABLE')]
                conn.close()

            if tables:
                self.option_source_name.configure(values=tables)
                self.option_source_name.set(tables[0])
            else:
                self.option_source_name.set("Tablo Yok")
        except Exception as e:
            self.log(f"Tablo hatası: {e}", "ERROR")

    # --- ANALİZ ---
    def start_analysis_thread(self):
        threading.Thread(target=self.run_analysis, daemon=True).start()

    def run_analysis(self):
        self.btn_analyze.configure(state="disabled", text="Analiz Yapılıyor...")
        self.btn_run.configure(state="disabled")
        for w in self.scroll_mapping.winfo_children(): w.destroy()

        try:
            conf = {"host": self.entry_host.get(), "db": self.entry_db.get(), "user": self.entry_user.get(),
                    "pass": self.entry_pass.get()}
            pg_url = f"postgresql+psycopg2://{conf['user']}:{conf['pass']}@{conf['host']}:5432/{conf['db']}"
            engine = create_engine(pg_url)
            inspector = inspect(engine)

            path = self.entry_file_path.get()
            name = self.option_source_name.get()
            source_cols = []

            if path.endswith(('.xlsx', '.xls')):
                source_cols = pd.read_excel(path, sheet_name=name, nrows=0).columns.tolist()
            elif path.endswith(('.mdb', '.accdb')):
                conn_str = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" rf"DBQ={path};")
                conn = pyodbc.connect(conn_str)
                df = pd.read_sql(f"SELECT TOP 1 * FROM [{name}]", conn)
                conn.close()
                source_cols = df.columns.tolist()

            target_table = self.entry_target_table.get()
            db_cols = []
            table_status = "YOK"
            if inspector.has_table(target_table):
                db_cols = [col['name'] for col in inspector.get_columns(target_table)]
                table_status = "VAR"

            headers = ["Kaynak Sütun", "Durum", "İşlem"]
            for i, h in enumerate(headers): ctk.CTkLabel(self.scroll_mapping, text=h, font=("Roboto", 12, "bold")).grid(
                row=0, column=i, padx=10, sticky="w")

            for i, col in enumerate(source_cols):
                r = i + 1
                if table_status == "YOK":
                    s_txt, s_col, d_txt = "✨ YENİ", "#2A9D8F", "Oluşturulacak"
                elif col in db_cols:
                    s_txt, s_col, d_txt = "✅ MEVCUT", "#2A9D8F", "Eklenecek"
                else:
                    s_txt, s_col, d_txt = "⛔ YOK", "#E76F51", "Atlanacak"

                ctk.CTkLabel(self.scroll_mapping, text=col).grid(row=r, column=0, padx=10, sticky="w")
                ctk.CTkLabel(self.scroll_mapping, text=s_txt, text_color=s_col).grid(row=r, column=1, padx=10,
                                                                                     sticky="w")
                ctk.CTkLabel(self.scroll_mapping, text=d_txt).grid(row=r, column=2, padx=10, sticky="w")

            self.lbl_mapping_status.configure(text=f"Analiz Tamamlandı: {len(source_cols)} sütun tarandı.")
            self.save_settings()
            self.btn_run.configure(state="normal")
            self.tab_view.set("Şema Röntgeni (X-Ray)")

        except Exception as e:
            self.log(f"Analiz Hatası: {e}", "ERROR")
            CTkMessagebox(title="Hata", message=str(e), icon="cancel")
        finally:
            self.btn_analyze.configure(state="normal", text="🔍 BAĞLANTIYI TEST ET & ANALİZ ET")

    # --- AKTARIM ---
    def stop_process(self):
        self.stop_event.set()
        self.btn_stop.configure(state="disabled", text="Durduruluyor...")
        self.log("Durdurma komutu verildi...", "WARNING")

    def start_transfer_thread(self):
        if not self.btn_run.cget("state") == "normal": return
        self.stop_event.clear()
        threading.Thread(target=self.run_transfer, daemon=True).start()

    def run_transfer(self):
        self.btn_run.configure(state="disabled")
        self.btn_stop.configure(state="normal", text="⛔ DURDUR")
        self.tab_view.set("Canlı Loglar")

        try:
            conf = {"host": self.entry_host.get(), "db": self.entry_db.get(), "user": self.entry_user.get(),
                    "pass": self.entry_pass.get()}
            pg_url = f"postgresql+psycopg2://{conf['user']}:{conf['pass']}@{conf['host']}:5432/{conf['db']}"
            engine = create_engine(pg_url)

            path = self.entry_file_path.get()
            name = self.option_source_name.get()
            target_table = self.entry_target_table.get()

            self.log(f"Veri okunuyor: {name}...", "INFO")
            self.lbl_progress.configure(text="Veri RAM'e yükleniyor...")
            self.progressbar.set(0)

            if path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(path, sheet_name=name)
            elif path.endswith(('.mdb', '.accdb')):
                conn_str = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" rf"DBQ={path};")
                conn = pyodbc.connect(conn_str)
                df = pd.read_sql(f"SELECT * FROM [{name}]", conn)
                conn.close()

            inspector = inspect(engine)
            if inspector.has_table(target_table):
                db_cols = [col['name'] for col in inspector.get_columns(target_table)]
                valid_cols = [c for c in df.columns if c in db_cols]
                df = df[valid_cols]

            total_rows = len(df)
            chunk_size = 5000
            self.log(f"Toplam {total_rows} satır. Başlıyor...", "INFO")

            inserted_count = 0
            for i in range(0, total_rows, chunk_size):
                if self.stop_event.is_set():
                    self.log("İşlem kullanıcı tarafından durduruldu!", "WARNING")
                    self.lbl_progress.configure(text=f"Durduruldu. ({inserted_count}/{total_rows})")
                    CTkMessagebox(title="İptal", message="İşlem durduruldu.", icon="warning")
                    return

                chunk = df.iloc[i: i + chunk_size]
                chunk.to_sql(target_table, engine, if_exists="append", index=False)
                inserted_count += len(chunk)

                progress = inserted_count / total_rows
                self.progressbar.set(progress)
                self.lbl_progress.configure(
                    text=f"Aktarılıyor... %{int(progress * 100)} ({inserted_count}/{total_rows})")

            self.log("🎉 İŞLEM BAŞARIYLA TAMAMLANDI!", "SUCCESS")
            self.lbl_progress.configure(text="Tamamlandı.")
            CTkMessagebox(title="Başarılı", message=f"{total_rows} satır aktarıldı!", icon="check")

        except Exception as e:
            self.log(f"HATA: {e}", "ERROR")
            CTkMessagebox(title="Hata", message=str(e), icon="cancel")
        finally:
            self.btn_run.configure(state="normal")
            self.btn_stop.configure(state="disabled")

    # --- AYARLAR ---
    def save_settings(self):
        data = {
            "host": self.entry_host.get(), "db": self.entry_db.get(),
            "user": self.entry_user.get(),
            "pass_enc": self.encrypt(self.entry_pass.get()),
            "target": self.entry_target_table.get()
        }
        try:
            with open(self.config_file, "w") as f:
                json.dump(data, f)
        except:
            pass

    def load_settings(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r") as f:
                    data = json.load(f)
                self.entry_host.insert(0, data.get("host", ""))
                self.entry_db.insert(0, data.get("db", ""))
                self.entry_user.insert(0, data.get("user", ""))
                self.entry_pass.insert(0, self.decrypt(data.get("pass_enc", "")))
                self.entry_target_table.insert(0, data.get("target", ""))
            except:
                pass

    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        icon = {"INFO": "ℹ️", "ERROR": "❌", "SUCCESS": "✅", "WARNING": "⚠️"}.get(level, "ℹ️")
        self.textbox_log.configure(state="normal")
        self.textbox_log.insert("end", f"[{timestamp}] {icon} {message}\n")
        self.textbox_log.see("end")
        self.textbox_log.configure(state="disabled")


if __name__ == "__main__":
    app = UltimateETLApp()
    app.mainloop()