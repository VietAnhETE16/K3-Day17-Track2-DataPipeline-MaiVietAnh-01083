# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Mai Việt Anh  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 66.3s
  run 2/3 … 65.2s
  run 3/3 … 68.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (100% tiêu chí chính và mở rộng đều hoàn thành xuất sắc)

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` tăng đột biến từ 12.480 lên 38.750 hàng sau mỗi lượt chạy (thừa 26.270 hàng), mỗi ticket bị lặp nhiều lần và checksum qua 3 lượt chạy hoàn toàn khác nhau. |
| **Nguyên nhân** | Model `gold_training_set` được cấu hình `materialized = 'incremental'` nhưng không khai báo `unique_key` và `incremental_strategy`, khiến dbt mặc định sinh ra câu lệnh `INSERT` thuần (append-only). Do nguồn CDC có các bản ghi cập nhật `op = 'u'` ở nhiều ngày khác nhau và scheduler/user chạy lại các partition ngày, dbt chèn thêm hàng mới thay vì ghi đè theo khoá thực thể `ticket_id`. |
| **Cách khắc phục** | - Trong `dbt/models/gold/gold_training_set.sql`: Khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`.<br>- Trong `dags/ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1` để tránh Airflow tự động chạy bù song song nhiều ngày gây xung đột ghi. |
| **Bằng chứng** | trước: 38.750 hàng (12.480 ticket bị lặp) · sau: 12.480 hàng (1 hàng / 1 ticket) · checksum 3 lượt: `8dd7c98653` / `8dd7c98653` / `8dd7c98653` (ổn định tuyệt đối). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` bị thiếu 455 hàng (chỉ đạt 8.645 / 9.100 hàng kỳ vọng). Các cặp `(event_date, customer_id)` bị thiếu tập trung ở các ngày trong quá khứ. |
| **P99 độ trễ đo được** | **2.73 ngày** (P50 = 0.13 ngày, P95 = 1.81 ngày, P99 = 2.73 ngày, Max = 2.94 ngày; 5.05% sự kiện đến trễ hơn 1 ngày). |
| **Lookback đã chọn** | **3 ngày** — vì P99 độ trễ là 2.73 ngày và giá trị trễ tối đa là 2.94 ngày (< 3 ngày), do đó cửa sổ lùi 3 ngày đảm bảo thu thập đầy đủ 100% dữ liệu đến trễ mà vẫn tối ưu tài nguyên tính toán. |
| **Nguyên nhân** | Dữ liệu sự kiện có độ trễ lớn từ nguồn (late-arriving data). Điều kiện incremental cũ `where event_date > (select max(event_date) from {{ this }})` chỉ xử lý những ngày lớn hơn ngày lớn nhất đã có trong bảng đích, do đó bỏ sót hoàn toàn các sự kiện thuộc ngày cũ nhưng đến kho muộn hơn 1–3 ngày. |
| **Cách khắc phục** | - Trong `dbt/models/gold/gold_feature_daily.sql`: Nới rộng điều kiện incremental thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`.<br>- Khai báo `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để việc tính toán lại các ngày trong cửa sổ lookback sẽ thực hiện ghi đè (upsert) thay vì cộng dồn số lượng. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng (đủ 14 ngày × 650 customer) · checksum 3 lượt: `3db448685c` / `3db448685c` / `3db448685c` (ổn định). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> **Trả lời:** P99 đại diện cho phân bố thời gian của 99% dữ liệu thông thường, giúp cân bằng tối ưu giữa độ đầy đủ của dữ liệu (data completeness) và chi phí tài nguyên tính toán/I/O (compute cost) ở mỗi chu kỳ chạy định kỳ. Nếu căn cứ theo `max` (hoặc mở rộng không giới hạn), một sự kiện cá biệt bị trễ cả tháng do sự cố mạng/sensor (outlier) sẽ ép toàn bộ pipeline phải quét lại toàn bộ lịch sử ở mọi lần chạy, làm tăng vọt chi phí và thời gian thực thi của đường ống. Trong lab này, `max = 2.94 ngày` vẫn nằm trong khoảng an toàn 3 ngày nên lookback 3 ngày vừa đạt tiêu chuẩn P99 vừa bao phủ toàn bộ dữ liệu.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Cột `silver_tickets.priority` có 6.606 hàng sai quy chuẩn (chứa nhiều giá trị NULL và các số ngoài khoảng như 0, 5, -1); bảng `quarantine_tickets` bị rỗng (0 / 312 hàng). |
| **Nguyên nhân** | Team backend thay đổi schema từ ngày 2026-08-10 (chuyển cách ghi priority từ số sang nhãn chuỗi 'urgent', 'high', 'medium', 'low'), đồng thời nguồn CDC phát sinh các bản ghi lỗi ('P1', 'unknown', '0', '5', '-1', '', NULL). Hàm `try_cast(priority_raw as integer)` ban đầu chuyển nhãn chuỗi thành NULL (vứt bỏ dữ liệu hợp lệ) nhưng lại chấp nhận các số 0, 5, -1; đồng thời bảng quarantine bị gán cứng `where false`. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ** (`1`, `2`, `3`, `4`): Đúng contract -> Giữ nguyên.<br>2. **Nhãn chuỗi** (`urgent`, `high`, `medium`, `low`): Schema evolution từ backend -> Map về số tương ứng (urgent=1, high=2, medium=3, low=4).<br>3. **Giá trị lỗi** (`P1`, `unknown`, `0`, `5`, `-1`, `''`, `NULL`): Dữ liệu lỗi -> Trả về `NULL` và đưa vào bảng `quarantine_tickets`. |
| **Cách khắc phục** | - `dbt/macros/normalize_priority.sql`: Dùng khối `CASE` phân loại và chuẩn hóa 3 nhóm giá trị, trả về `NULL` cho nhóm 3; bổ sung `priority_reject_reason`.<br>- `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi lỗi (`where priority_clean is not null`) **trước khi** thực hiện `row_number()`, giúp ticket có bản ghi mới nhất bị lỗi vẫn giữ được trạng thái hợp lệ trước đó (đủ 12.480 ticket).<br>- `dbt/models/silver/quarantine_tickets.sql`: Đổi điều kiện thành `where {{ normalize_priority('priority_raw') }} is null`.<br>- `dbt/models/silver/schema.yml`: Bật `contract: enforced: true` và thêm tests `not_null`, `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (100% khớp kỳ vọng) · `silver_tickets` đủ 12.480 ticket · `dbt test` = 11/11 pass (thêm 2 tests mới). |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Trả lời:**
> 1. **Chặn ở Bronze hay Silver:** Nên tiếp nhận toàn bộ dữ liệu thô tại tầng **Bronze** (nguyên tắc raw immutability) và thực hiện chặn/chuẩn hóa/quarantine tại tầng **Silver**. Nếu từ chối bản ghi lỗi ngay tại Bronze, dữ liệu gốc sẽ bị mất hoàn toàn, gây khó khăn cho việc truy vết nguồn gốc (audit/debugging), phân tích sự cố và khôi phục (replay) dữ liệu sau này.
> 2. **Không dừng pipeline khi gặp row lỗi:** 312 bản ghi CDC lỗi không được phép làm tê liệt toàn bộ hệ thống và chặn hơn 130.000 sự kiện cùng 31.200 tài liệu hoàn toàn bình thường đến các ứng dụng downstream (RAG index, classifier, routing agent). Phân nhánh bản ghi lỗi vào bảng `quarantine_tickets` đóng vai trò như Dead Letter Queue (DLQ), vừa đảm bảo chất lượng dữ liệu sạch cho tầng Gold, vừa duy trì độ sẵn sàng cao (High Availability) cho đường ống và cung cấp hàng đợi rõ ràng cho kỹ sư vận hành xử lý ngoại lệ.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **Cả hai bài: Bài A (+5 điểm) và Bài B (+5 điểm)** |
| **Nguyên nhân** | - **Bài A:** Dataset `data/gold_events/` gồm 5.000 file Parquet nhỏ rời rạc (small-file problem), không partition theo ngày và không cluster theo khách hàng; câu truy vấn sử dụng predicate non-sargable `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` buộc DuckDB phải quét toàn bộ 5.000 file (~5.000.000 rows scanned).<br>- **Bài B:** Thứ tự cũ trong `consume()` gọi `consumer.commit()` trước khi `write_batch()` (at-most-once semantics), dẫn đến mất dữ liệu khi tiến trình bị kill ở giữa batch. Nếu đảo lại mà dùng `INSERT` thuần thì khi restart sẽ làm nhân bản dữ liệu (at-least-once). |
| **Cách khắc phục** | - **Bài A:** Triển khai `tools/compact.py` gom 5.000 file nhỏ thành 14 file partition theo `event_date` tại `data/gold_events_v2/`, sắp xếp `ORDER BY customer_name, event_time` với `ROW_GROUP_SIZE 2048`. Cập nhật `queries/dashboard.sql` đọc với `hive_partitioning = 1` và filter sargable `event_date = '2026-08-09'`.<br>- **Bài B:** Khai báo `event_id varchar primary key` trong DDL bảng `bronze_events_stream`, cập nhật `write_batch` sử dụng `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...`, và trong `consume()` thực hiện `write_batch()` trước khi `consumer.commit()` (kết hợp at-least-once transport với idempotent upsert). |
| **Bằng chứng** | - **Bài A:** `rows scanned` giảm từ 5.000.000 xuống 9.324 (giảm **536.3×**, mục tiêu ≥ 10×) · Số file giảm từ 5.000 xuống 14 file · `result hash` giữ nguyên `4379e4c5d9f3` · Thời gian query giảm từ ~99.000 ms xuống 13.2 ms.<br>- **Bài B:** `make crash-test` đạt tuyệt đối: Không mất bản ghi (✓), Không trùng bản ghi (✓), C == A = 20.000 hàng / 20.000 event_id (✓) -> **BÀI MỞ RỘNG B: ĐẠT ✓**. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra tính **idempotency** của toàn bộ các incremental models (định nghĩa grain thực thể, khai báo `unique_key`, lựa chọn chiến lược `merge`/`delete+insert`) và các tham số điều phối scheduler (`catchup`, `max_active_runs`). |
| 2 | Đo đạc **phân bố độ trễ nạp dữ liệu** (P95, P99 latency giữa `event_time` và `_ingested_at`) để thiết lập lookback window phù hợp cho dữ liệu đến muộn, kết hợp với cơ chế ghi đè idempotent tránh trùng lặp. |
| 3 | Kiểm tra hệ thống **Data Contract** tại tầng Silver, phân loại chính xác giữa Schema Evolution và Bad Data, thiết lập cơ chế Dead Letter Queue (**Quarantine**) để cách ly lỗi mà không làm gián đoạn pipeline, cùng bộ kiểm thử dữ liệu toàn diện (`dbt test`). |

---

## 6 · Bảng tự chấm nhanh (Self-Assessment)

| Hạng mục kiểm tra | Của tôi | Kỳ vọng | Đạt (✓/✗) |
|---|---|---|:---:|
| `gold_training_set` — số hàng | **12.480** | 12.480 | ✓ |
| `gold_training_set` — ổn định 3 lượt | **8dd7c98653** | Giống nhau 3 lượt | ✓ |
| `gold_feature_daily` — số hàng | **9.100** | 9.100 | ✓ |
| `gold_feature_daily` — ổn định 3 lượt | **3db448685c** | Giống nhau 3 lượt | ✓ |
| `gold_doc_chunks` — số hàng | **31.200** | 31.200 | ✓ |
| `gold_doc_chunks` — ổn định 3 lượt | **92d8e50131** | Giống nhau 3 lượt | ✓ |
| `quarantine_tickets` — số hàng | **312** | 312 | ✓ |
| `quarantine_tickets` — ổn định 3 lượt | **ebb89036fb** | Giống nhau 3 lượt | ✓ |
| `silver_tickets` — số ticket | **12.480** | 12.480 | ✓ |
| `silver_tickets.priority` | **Sạch ∈ 1..4, không NULL** | 1..4, không NULL | ✓ |
| `dbt test` | **11/11 pass** | pass, > 9 test | ✓ |
| P99 độ trễ đo được | **2.73 ngày** (Lookback 3 ngày) | (ghi số) | ✓ |
| Bài mở rộng A (Dashboard compaction) | **5.000.000 → 9.324 (536.3×)** | giảm ≥ 10× | ✓ |
| Bài mở rộng B (Crash recovery) | **20.000 / 20.000 (C == A)** | Đạt | ✓ |
| **Tổng verify** | **4 / 4 tiêu chí đạt** | 4 / 4 tiêu chí | **✓ (110/100)** |

