# 🚀 TGH Data Integrator (Enterprise ETL Tool)

Modern, güvenli ve kullanıcı dostu bir Veri Aktarım (ETL) Aracı.
Bu proje, Excel ve Access veritabanlarında tutulan milyonlarca satırlık veriyi, şema uyumsuzluklarını (schema drift) otomatik algılayarak PostgreSQL veritabanına güvenli bir şekilde aktarmak için geliştirilmiştir.

## 🌟 Öne Çıkan Özellikler

* **🔍 X-Ray Şema Analizi:** Kaynak ve hedef tabloyu karşılaştırır, sütun uyuşmazlıklarını görsel olarak raporlar.
* **🛡️ Enterprise Security:** Veritabanı şifrelerini Base64 ile şifreleyerek saklar.
* **⚡ Multi-Threaded Performance:** Arayüz donmadan 10+ Milyon satır veriyi chunk (parça) bazlı aktarır.
* **🔄 Auto-Detect:** Access (.mdb/.accdb) veya Excel (.xlsx) dosya türünü otomatik algılar.
* **⛔ Panic Button:** İşlem sırasında herhangi bir sorun olursa aktarımı güvenle durdurma (Graceful Shutdown).

## 🛠️ Kullanılan Teknolojiler

* **Dil:** Python 3.13
* **UI:** CustomTkinter (Modern Dark Mode Arayüz)
* **Data:** Pandas, SQLAlchemy, PyODBC
* **Build:** PyInstaller (Standalone .exe)

## 📦 Kurulum

```bash
git clone [https://github.com/kullaniciadin/TGH-Data-Integrator.git](https://github.com/kullaniciadin/TGH-Data-Integrator.git)
cd TGH-Data-Integrator
pip install -r requirements.txt
python main_guiV2.py