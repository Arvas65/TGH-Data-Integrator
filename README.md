# TGH Data Integrator

**Oracle → PostgreSQL veri aktarımını otomatikleştiren masaüstü uygulaması.**
Bir hastanenin Karar Destek Birimi'nin her gün elle yürüttüğü ~3 saatlik veri hazırlama sürecini **30 dakikaya** indirdi. Aktif olarak kullanılmaktadır.

## Problem

Karar destek ekibi, raporlama için gereken verileri Oracle (HBYS) ortamından elle sorgulayıp PostgreSQL tarafına taşıyordu. Süreç her gün tekrarlanıyor, yaklaşık 3 saat sürüyor ve insan hatasına açıktı.

## Çözüm

Tek tıkla çalışan, zamanlanabilir bir masaüstü aktarım aracı:

- **Kaynak → Hedef eşleme:** Oracle'dan seçilen sorgu/tabloların PostgreSQL şemasına aktarımı
- **Delta / Full senkronizasyon:** Tam aktarım veya yalnızca değişen kayıtların taşınması
- **Güvenli konfigürasyon:** Bağlantı bilgileri Fernet (simetrik şifreleme) ile şifrelenmiş olarak saklanır — düz metin şifre yok
- **GUI:** CustomTkinter ile teknik olmayan kullanıcıların da çalıştırabileceği arayüz
- **Paketleme:** PyInstaller ile tek dosyalık .exe; kullanıcı bilgisayarında Python kurulumu gerektirmez

## Mimari

```
Oracle (HBYS)  ──►  Extract (oracledb)  ──►  Transform (pandas)  ──►  Load (psycopg2)  ──►  PostgreSQL
                          │
                    Fernet ile şifreli config (bağlantı bilgileri)
```

## Kullanılan teknolojiler

Python · oracledb · psycopg2 · pandas · cryptography (Fernet) · CustomTkinter · PyInstaller

## Sonuç

| | Önce | Sonra |
|---|---|---|
| Günlük süre | ~3 saat (manuel) | ~30 dakika (otomatik) |
| İnsan hatası riski | Yüksek | Düşük |
| Şifre yönetimi | Düz metin | Şifreli (Fernet) |

## Not

Bu depo, kurum içi hassas bilgiler (bağlantı bilgileri, gerçek şema/tablo adları, veri örnekleri) temizlenerek paylaşılmıştır. Konfigürasyon örnek dosya (`config.example`) üzerinden gösterilmektedir.
