# PM5110 register profile

Sumber resmi: [Schneider FA234017](https://www.se.com/ie/en/faqs/FA234017/) dan [PM5100-PM5300 Public Register List](https://download.schneider-electric.com/files?p_enDocType=User+guide&p_File_Name=PM5100_PM5300_PMC+Register+List.xls&p_Doc_Ref=PM5100-PM5300_PublicRegisterList). Kolom `PM5110/11` pada workbook menandai parameter berikut didukung. Lembar `Data Types` menjelaskan sentinel dan encoding PF empat kuadran.

Register pada dokumentasi menggunakan penomoran satu-based. Alamat PDU yang dikirim adalah **register dikurangi satu**. FLOAT32 menggunakan word most-significant-first. INT64 energi mengikuti format khusus Schneider: UINT32 rendah lebih dahulu, lalu UINT32 tinggi; di dalam setiap UINT32, word/byte paling signifikan lebih dahulu. Semua pembacaan memakai FC03; tidak ada penulisan konfigurasi.

| Parameter | Register | Word | Tipe | Satuan |
|---|---:|---:|---|---|
| Product ID PM5110 = 15270 | 90 | 1 | UINT16 | — |
| Current A/B/C | 3000/3002/3004 | 2 masing-masing | FLOAT32 | A |
| Voltage AB/BC/CA | 3020/3022/3024 | 2 masing-masing | FLOAT32 | V |
| Voltage AN/BN/CN | 3028/3030/3032 | 2 masing-masing | FLOAT32 | V |
| Active power total | 3060 | 2 | FLOAT32 | kW |
| Reactive power total | 3068 | 2 | FLOAT32 | kvar |
| Apparent power total | 3076 | 2 | FLOAT32 | kVA |
| Total power factor | 3084 | 2 | 4Q FP PF | — |
| Frequency | 3110 | 2 | FLOAT32 | Hz |
| Active energy delivered / import | 3204 | 4 | INT64 | Wh |
| Active energy received / export | 3208 | 4 | INT64 | Wh |

Konversi PF: nilai mentah `x>1` menjadi `2-x`; `x<-1` menjadi `-2-x`; selain itu tetap `x`. API menyajikan PF bertanda dalam rentang −1 sampai 1, tanpa label leading/lagging. Skala parameter lain 1. Energi tidak dikonversi ke float.

`NaN`, infinity, sentinel INT64 `0x8000000000000000`, serta energi negatif diperlakukan tidak tersedia. Sampel baru selalu berawal dengan semua nilai `null`. Field yang gagal memiliki alasan pada `errors`.

Pemilihan profil berdasar product ID bukan pengenalan otomatis firmware. Saat commissioning cocokkan word order, arah daya/energi, satuan, dan nilai display dengan firmware perangkat. Jangan mengganti offset atau word order hanya untuk membuat nilai terlihat masuk akal.
