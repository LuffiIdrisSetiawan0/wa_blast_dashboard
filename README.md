## WA Blast Dashboard

Dashboard Streamlit untuk menampilkan KPI WhatsApp BSP dengan pipeline upload → cleaning → penyimpanan terpusat (Supabase) serta fallback lokal untuk prototyping.

### Arsitektur Singkat
- **Streamlit** menangani UI upload + dashboard. Semua user melihat dataset terbaru atau memilih periode lama dari riwayat.
- **Pandas pipeline** (`app/data_processing`) menormalisasi file CSV/XLSX, merapikan kolom waktu, menghitung KPI, dan menyiapkan data visual.
- **Supabase** (opsional) menyimpan file mentah di Storage, hasil bersih di bucket lain, dan metadata/metrics di tabel `reports`. Jika kredensial tidak diisi, aplikasi otomatis memakai penyimpanan lokal `data/local_store`.

### Struktur Folder
```
app/
  config.py                 # loader environment dan default bucket/table
  data_processing/          # cleaning + agregasi KPI
  dashboard/components.py   # komponen visual Streamlit (KPI, chart, tabel)
  models/report.py          # dataclass metadata & metrics
  services/supabase_service.py  # abstraksi penyimpanan Supabase/lokal
  utils/                    # tersedia untuk helper tambahan
data/
  multichannel-transaction-1763082445610.csv  # contoh file BSP
scripts/
  bootstrap_sample.py       # seed data lokal dari file contoh
streamlit_app.py            # entry utama aplikasi
requirements.txt
.env.example
```

### Persiapan Lingkungan
1. Buat virtualenv lalu instal dependensi:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Salin `.env.example` menjadi `.env` dan isi variabel sesuai Supabase Anda:
   - `SUPABASE_URL` dan `SUPABASE_SERVICE_KEY`.
   - Nama bucket untuk file mentah dan bersih (atau biarkan default `wa_reports_raw/clean`).
   - Nama tabel metadata (default `reports`). Tabel perlu minimal kolom:
     ```sql
     create table reports (
       id text primary key,
       period_label text,
       source_filename text,
       uploaded_by text,
       uploaded_at timestamptz,
       raw_storage_path text,
       clean_storage_path text,
       notes text,
       metrics jsonb
     );
     ```
3. Jika belum ingin memakai Supabase, biarkan variabel kosong. Sistem akan memakai folder `data/local_store`.

### Bootstrap Data Lokal
Jalankan script berikut agar dashboard langsung memiliki dataset contoh:
```bash
python scripts/bootstrap_sample.py --period 2025-11-week2
```
Script membaca `data/multichannel-transaction-*.csv`, menjalankan cleaning, lalu menyimpan raw/clean beserta metadata ke penyimpanan lokal.

### Menjalankan Streamlit
```bash
streamlit run streamlit_app.py
```
Fitur utama:
- Panel kiri: riwayat periode + form upload file baru (CSV/XLSX hingga ±25 MB).
- Upload baru → file disimpan (Supabase atau lokal), cleaned dataset di-cache, KPI & grafik otomatis ter-refresh.
- Halaman utama: kartu KPI, chart status, trend pengiriman harian, tabel 20 pesan terbaru, dan opsi unduh CSV bersih.

### Deployment Gratis
- Push repo ke GitHub → deploy ke **Streamlit Community Cloud** atau **HuggingFace Spaces**. Tambahkan variabel lingkungan Supabase via dashboard hosting.
- Ketahui bahwa instans free akan *sleep* setelah ±15 menit idle; pengunjung pertama akan merasakan cold start beberapa detik.

### Langkah Lanjut
- Tambahkan validasi skema file (mis. mengunci kolom wajib) pada `clean_transactions`.
- Simpan agregasi tambahan (e.g. KPI per area/operator) ke tabel khusus untuk query cepat.
- Kalau ingin halaman publik read-only, pisahkan halaman upload (mis. hanya dijalankan admin secara lokal) atau beri kunci rahasia sederhana.
