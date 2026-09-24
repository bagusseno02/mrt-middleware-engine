# PM5110 Collector

Service Go untuk membaca Schneider PowerLogic **PM5110**, menyimpan hasil ke PostgreSQL, dan menyediakan REST API. Mendukung Modbus RTU melalui RS-485 serta Modbus TCP **melalui gateway TCP–RTU**. Semua operasi meter memakai function code `03` (read holding registers).

```text
PM5110 -- RS-485 / gateway TCP -- collector Go -- SQLite outbox -- PostgreSQL
                                                                    |
                                                               REST API
```

## Menjalankan dengan Docker Compose

Prasyarat: Docker Engine dengan Compose v2, gateway Modbus TCP, serta parameter komunikasi meter yang sudah diketahui.

1. Salin `.env.example` menjadi `.env`. Ganti `POSTGRES_PASSWORD` dan `API_KEY` dengan **dua nilai berbeda** hasil `openssl rand -hex 32`. Gunakan password hex agar aman dimasukkan ke URL koneksi.
2. Edit `config.yaml`: alamat gateway, `unit_id`, nama, dan lokasi meter. Alamat `192.168.1.100:502` hanya contoh, bukan perangkat yang sudah ditemukan.
3. Jalankan:

   ```sh
   docker compose up --build -d
   docker compose logs -f collector
   ```

Compose menjalankan PostgreSQL, migrasi satu kali, lalu collector. PostgreSQL dan SQLite memakai named volume persisten. API diterbitkan pada `127.0.0.1:8080`; database pada `127.0.0.1:5432`. Jangan gunakan `docker compose down -v` bila data perlu dipertahankan.

Untuk gateway eksternal yang hanya mendukung serial transparan/RTU-over-TCP, transport `tcp` ini tidak cocok: gateway harus mendukung Modbus TCP dengan header MBAP dan meneruskan unit ID.

### RS-485 langsung

Gunakan `config.rtu.example.yaml` sebagai isi `config.yaml`. Untuk Docker, ubah `http_address` menjadi `0.0.0.0:8080`, lalu:

```sh
export SERIAL_GID=$(stat -c '%g' /dev/ttyUSB0)
docker compose -f compose.yaml -f compose.rtu.example.yaml up --build -d
```

Sesuaikan device mapping jika adaptor berbeda. Pilih adaptor dengan pengaturan arah transmisi otomatis. Untuk instalasi native, gunakan path stabil `/dev/serial/by-id/...` dan beri user service akses grup serial. Baud, parity, stop bits, serta slave ID harus sama dengan pengaturan meter; contoh `19200/E/1` bukan deteksi otomatis.

Beberapa meter pada satu bus/gateway harus menggunakan **connection ID yang sama**, dengan unit ID berbeda. Collector menserialkan permintaan pada koneksi tersebut; koneksi berbeda berjalan bersamaan. Satu proses memiliki satu file antrean. Jangan menjalankan dua collector untuk bus yang sama.

## Menjalankan native

Prasyarat: Go 1.26 atau lebih baru dan PostgreSQL 16 atau lebih baru. Contoh di bawah untuk shell Linux; `.env` tidak dibaca otomatis oleh binary.

```sh
go mod download
go build -o bin/pmcollector ./cmd/pmcollector
export DATABASE_URL='postgres://pmcollector:PASSWORD@localhost:5432/pmmonitor?sslmode=disable'
export API_KEY='YOUR_RANDOM_API_KEY_AT_LEAST_32_CHARACTERS'
./bin/pmcollector check -config config.yaml
./bin/pmcollector migrate
./bin/pmcollector serve -config config.yaml
```

Pada Windows gunakan `$env:DATABASE_URL`, `$env:API_KEY`, dan serial port seperti `COM3`. Untuk akses database di jaringan luar host, atur TLS melalui parameter `DATABASE_URL` sesuai deployment. TLS API diterminasi oleh reverse proxy di jaringan internal.

### Migrasi

- `migrations/001_init.up.sql`: tabel `meters`, `measurements`, constraint, dan indeks histori.
- `migrations/001_init.down.sql`: rollback destruktif yang disediakan untuk operator; aplikasi tidak menjalankannya otomatis.
- Subcommand `migrate` mengeksekusi SQL ter-embed dalam transaksi, mengambil advisory lock, serta mencatat versi di `schema_migrations`. Aman dijalankan berulang. Hanya membutuhkan `DATABASE_URL`.
- Gunakan subcommand untuk migrasi naik agar tabel versi ikut dikelola. Setelah backup dan keputusan operator, rollback dapat dijalankan dengan `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 --single-transaction -f migrations/001_init.down.sql`.
- Akun migrasi membutuhkan hak membuat tabel/indeks. Tidak ada migrasi implisit ketika collector mulai berjalan. Jika schema belum ada, antrean tetap menerima data, API data belum siap, dan delivery mencoba ulang.

Data PostgreSQL **tidak dihapus otomatis**. Pantau penggunaan disk dan siapkan backup database serta volume antrean.

## Konfigurasi

| Parameter | Default / makna |
|---|---|
| `http_address` | `127.0.0.1:8080`; contoh Docker memakai `0.0.0.0:8080` |
| `queue_path` | `data/queue.db` |
| `queue_max_bytes` | 1 GiB total anggaran database SQLite dan rollback journal |
| `connections[].transport` | Wajib `rtu` atau `tcp` |
| `connections[].timeout` | 2 detik per permintaan, maksimal 30 detik |
| `connections[].retries` | 1 retry; dapat disetel 0–3 |
| `connections[].baud/parity/stop_bits` | `19200/E/1`, data bits selalu 8 |
| `meters[].interval` | 10 detik setelah polling meter selesai, minimal 1 detik |
| `meters[].energy` | `true` pada contoh; aktifkan pembacaan energi INT64 |
| `DATABASE_URL` | Environment wajib, tidak dicatat ke log |
| `API_KEY` | Environment wajib, minimal 32 karakter |

Tidak ada hot reload; restart service setelah konfigurasi berubah. Hasil setiap polling memakai timestamp host UTC (bukan jam internal meter). Pengambilan register berurutan bukan snapshot simultan; `observed_at` dan `completed_at` menunjukkan jendela pengukuran. Sinkronkan waktu host.

ID meter/koneksi terdiri dari 1–64 karakter: huruf/angka pada karakter pertama, lalu huruf, angka, titik, underscore, atau tanda minus.

### Antrean, gangguan, dan shutdown

- Hasil polling ditulis ke SQLite dengan `synchronous=FULL` sebelum dikirim ke PostgreSQL. Setiap sampel memakai UUID tetap. Commit PostgreSQL diikuti acknowledgement lokal; replay menggunakan `ON CONFLICT(id) DO NOTHING`.
- SQLite memakai rollback journal. Sekitar separuh anggaran disk dicadangkan untuk journal sehingga batas 1 GiB tidak berarti 1 GiB payload. File database dapat mempertahankan ukuran setelah pengosongan; halaman bebas dipakai kembali.
- Jika antrean penuh atau penulisan disk gagal, polling berhenti sementara. Delivery tetap mencoba mengosongkan antrean. `/health/ready` mengembalikan `503`; data yang sudah tersimpan tidak dibuang.
- PostgreSQL terputus: sampel tetap diantrekan, retry bertahap maksimal 30 detik, lalu dikirim saat pulih. Meter terputus: simpan kualitas `failed`; hasil parsial memakai `partial`. Nilai yang tidak tersedia tetap `null`, tidak diisi nol atau nilai lama.
- Polling menggunakan fixed delay, tanpa menumpuk siklus yang terlewat. Timeout dan berbagi bus dapat memperpanjang interval efektif.
- SIGTERM/SIGINT menghentikan polling, memberi kesempatan menyimpan sampel yang sedang diproses, dan menutup koneksi. Sampel yang belum dapat ditulis karena disk rusak saat shutdown dicatat sebagai `shutdown with unpersisted sample`; tidak ada jaminan untuk data yang belum berhasil disimpan secara lokal.
- `live` hanya memeriksa proses. `ready` memeriksa schema/koneksi PostgreSQL dan kemampuan menerima data antrean. Meter offline terlihat dari kualitas data, bukan dari health proses.

## REST API

Semua `/v1/*` memakai `Authorization: Bearer <API_KEY>`. Health endpoint tidak membutuhkan autentikasi dan hanya menampilkan status. JSON memakai nama parameter beserta satuannya; timestamp selalu UTC.

```sh
curl http://localhost:8080/health/ready
curl -H "Authorization: Bearer $API_KEY" http://localhost:8080/v1/meters
curl -H "Authorization: Bearer $API_KEY" http://localhost:8080/v1/meters/pm5110-01/latest
curl -G -H "Authorization: Bearer $API_KEY" \
  --data-urlencode 'from=2026-09-23T00:00:00Z' \
  --data-urlencode 'to=2026-09-24T00:00:00Z' \
  --data-urlencode 'limit=100' \
  http://localhost:8080/v1/meters/pm5110-01/measurements
```

| Endpoint | Perilaku |
|---|---|
| `GET /v1/meters` | Meter yang ada dalam konfigurasi saat ini, termasuk interval |
| `GET /v1/meters/{id}/latest` | Percobaan terbaru, termasuk yang gagal; `stale=true` setelah tiga interval |
| `GET /v1/meters/{id}/measurements` | Riwayat menurun berdasarkan `(observed_at, id)` |
| `GET /health/live` | `200` selama HTTP server berjalan |
| `GET /health/ready` | `200` siap, `503` database/antrean belum siap |

Riwayat membutuhkan `from` dan `to` RFC3339, dengan rentang positif maksimal 31 hari. Batas bawah inklusif dan batas atas eksklusif. `limit` default 100, maksimal 1.000. Respons berbentuk `{"items": [...], "next_cursor": "..."}`. Kirim cursor berikutnya dengan **meter dan from/to yang sama**; `next_cursor=null` berarti halaman terakhir. Riwayat kosong menghasilkan `200` dengan array kosong.

Status: `400` parameter salah, `401` token tidak valid, `404` meter atau data terbaru tidak ditemukan, `503` database tidak tersedia. API membaca PostgreSQL, sehingga data dalam antrean belum muncul pada API. Meter yang dihapus dari konfigurasi tidak ditampilkan API; histori database dan backlog tetap dipertahankan.

Energi dikembalikan sebagai string desimal Wh, misalnya `"energy_import_wh":"89550842"`, dan disimpan sebagai PostgreSQL `BIGINT`. Ini mempertahankan ketelitian counter melebihi batas integer aman JavaScript. Bagi dengan 1.000 menggunakan aritmetika desimal untuk tampilan kWh.

## Profil register dan commissioning

Detail sumber, alamat, dan decoding ada di [docs/registers.md](docs/registers.md). Collector memverifikasi product ID `15270` pada register `90` sebelum membaca pengukuran. Model lain tidak akan dibaca dengan profil ini.

Sebelum menyatakan integrasi lapangan selesai:

1. Pastikan model PM5110 dan catat firmware. Cocokkan daftar register resmi untuk firmware tersebut.
2. Periksa wiring RS-485, terminasi, polaritas, slave ID unik, dan konfigurasi komunikasi. Pekerjaan panel listrik dilakukan personel yang berwenang.
3. Pastikan pengaturan sistem listrik serta rasio CT/PT pada meter sesuai instalasi. Aplikasi menggunakan nilai yang sudah diskalakan meter tanpa mengalikan CT/PT kembali.
4. Bandingkan tegangan, arus, daya, frekuensi, faktor daya, dan energi dengan display meter pada beban stabil. Tegangan fasa-netral mungkin `null` jika wiring tidak menyediakannya.
5. Periksa import/export terhadap arah energi yang diketahui dan bandingkan energi Wh/1.000 dengan kWh display. Konfirmasi urutan word sebelum memakai data untuk pelaporan.
6. Verifikasi insert PostgreSQL dan API; putuskan koneksi database untuk memastikan antrean bertambah, lalu sambungkan kembali dan cek tidak ada duplikasi.
7. Simpan hasil pada [docs/commissioning.md](docs/commissioning.md).

Tes otomatis memakai simulator, bukan perangkat lapangan. Akses meter fisik diperlukan untuk commissioning.

## Pengujian

```sh
go test ./...
go vet ./...
# Gunakan database test: tes membuat dan menghapus schema pmtest_* tersendiri.
export TEST_DATABASE_URL='postgres://test:test@localhost:5432/test?sslmode=disable'
go test ./...
# Membutuhkan toolchain C untuk race detector pada platform yang memerlukannya.
go test -race ./...
```

Tanpa `TEST_DATABASE_URL`, tes PostgreSQL ditandai **skip**, bukan bukti integrasi database berhasil. CI menyediakan PostgreSQL 17 dan menjalankan suite dengan race detector. Tes mencakup decoding/PF/INT64, model salah, kualitas parsial, frame RTU/CRC, timeout/reconnect TCP, antrean penuh/restart/replay, autentikasi, pagination, migrasi, dan deduplikasi. Linux juga menguji driver serial RTU menggunakan pseudo-terminal.

Untuk menjalankan seluruh suite pada Linux dengan PostgreSQL terisolasi, tanpa menyiapkan database native:

```sh
docker compose -p pm5110-tests -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from test
docker compose -p pm5110-tests -f compose.test.yaml down -v
```

Project Compose test terpisah dari deployment dan tidak menerbitkan port database. Penghapusan di perintah kedua hanya untuk lingkungan test tersebut.

Struktur: `cmd/pmcollector` entrypoint; `internal/config`, `meter`, `transport`, `collector`, `queue`, `postgres`, dan `api` memisahkan tanggung jawab. Migrasi berada pada `migrations`.
# mrt-middleware-engine
