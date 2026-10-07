# BÀI THỰC HÀNH HADOOP MAPREDUCE (IS330 - DỮ LIỆU LỚN)

## 📌 Đề tài: Phân tích doanh thu đơn hàng thương mại điện tử bằng HDFS & Hadoop MapReduce

- **Sinh viên thực hiện:** Nguyễn Văn Khải
- **Mã sinh viên:** A47340
- **Học phần:** IS330 - Dữ liệu Lớn (Big Data)
- **Trường:** Đại học Thăng Long (Thang Long University)
- **GitHub Repository:** [https://github.com/NgVanKhai/hadoop](https://github.com/NgVanKhai/hadoop)

---

## 🚀 1. Tổng quan bài toán

Hệ thống xử lý phân tích dữ liệu bán hàng trực tuyến thương mại điện tử tháng 09/2026 (`orders_2026_09.csv`), tổng hợp tổng doanh thu cho từng nhóm sản phẩm (`category`) với điều kiện giao dịch thành công (`status = 'SUCCESS'`).

Thuật toán được xây dựng theo mô hình **Hadoop Streaming** sử dụng **Python 3**:
- **Mapper (`mapper_revenue.py`):** Đọc từng dòng CSV, phân tách trường, lọc chỉ lấy bản ghi có trạng thái `SUCCESS`, chuẩn hóa số tiền (`amount`), và phát ra cặp khóa - giá trị: `<category>\t<amount>`.
- **Shuffle & Sort:** Hadoop tự động phân phối và sắp xếp dữ liệu theo `category` trước khi chuyển đến Reducer.
- **Reducer (`reducer_revenue.py`):** Nhận luồng dữ liệu đã được gom theo từng khóa `category`, cộng dồn tổng doanh thu và xuất ra: `<category>\t<total_revenue>`.

---

## 📊 2. Kết quả tính toán & Bảng Top 3 Doanh thu

### Kết quả đầu ra MapReduce (`part-00000` / `result_revenue_by_category.txt`):
```text
Beauty          138350000
Book            25310000
Electronics     4386400000
Fashion         578900000
Grocery         72920000
Home & Living   492150000
Sports          93610000
```

### Bảng xếp hạng Top 3 ngành hàng có doanh thu cao nhất:

| Hạng | Ngành hàng (`category`) | Doanh thu (VND) | Tỷ trọng | Nhận xét phân tích |
| :---: | :--- | :---: | :---: | :--- |
| **1** | **Electronics** | **4.386.400.000** | **74,48%** | Chiếm ưu thế tuyệt đối do giá trị trung bình trên đơn (AOV) cao (laptop, smartphone, linh kiện điện tử). |
| **2** | **Fashion** | **578.900.000** | **9,83%** | Số lượng đơn hàng lớn, tần suất mua lặp lại cao, hưởng lợi mạnh từ các chiến dịch Mega Sale. |
| **3** | **Home & Living** | **492.150.000** | **8,36%** | Đồ gia dụng, tiện ích đời sống duy trì nhu cầu ổn định, giá thành tầm trung. |

- **Tổng doanh thu giao dịch thành công (SUCCESS):** **5.887.640.000 VND** (~5,888 tỷ đồng).
- **Tỷ trọng đóng góp của Top 3:** Đạt **92,67%** tổng doanh thu toàn hệ thống.

---

## 📁 3. Cấu trúc thư mục dự án

```text
A47340_NguyenVanKhai_HadoopLab/
├── README.md                           # Giới thiệu tổng quan và kết quả dự án
├── README.txt                          # Hướng dẫn nộp bài tóm tắt
├── BAO_CAO_THUC_HANH.md                # Báo cáo tổng hợp và minh chứng kỹ thuật
├── BAO_CAO_THUC_HANH_HADOOP_MAPREDUCE.md# Báo cáo phân tích chuyên sâu 5 câu hỏi lý thuyết
├── mapper_revenue.py                   # Mã nguồn Mapper (Python 3)
├── reducer_revenue.py                  # Mã nguồn Reducer (Python 3)
├── orders_2026_09.csv                  # Tập dữ liệu giao dịch thương mại điện tử
├── run_hadoop_lab.sh                   # Script tự động hóa toàn bộ quy trình trên Hadoop
├── part-00000                          # File đầu ra kết quả MapReduce từ HDFS
├── result_revenue_by_category.txt      # Kết quả xuất ra định dạng văn bản
└── evidence_logs/                      # Thư mục lưu 8 file log minh chứng thực thi:
    ├── 00_env_check.log                # Kiểm tra phiên bản Hadoop 3.3.6 & Python 3.10
    ├── 01_hdfs_input.log               # Tạo thư mục /user/labex/input và nạp dữ liệu
    ├── 02_local_test.log               # Kiểm thử luồng xử lý bằng pipeline cục bộ
    ├── 03_mapreduce_job.log            # Log quá trình submit & hoàn thành job MapReduce
    ├── 04_hdfs_output_list.log         # Liệt kê thư mục đầu ra trên HDFS (_SUCCESS, part-00000)
    ├── 05_result_preview.log           # Xem trước kết quả trực tiếp từ HDFS
    ├── 06_download_result.log          # Tải kết quả từ HDFS về máy trạm
    └── 07_final_report.log             # Tổng kết thống kê doanh thu và tỷ trọng
```

---

## ⚙️ 4. Hướng dẫn chạy

### Cách 1: Chạy tự động với kịch bản `run_hadoop_lab.sh` trên môi trường Hadoop (Linux)
```bash
# Phân quyền thực thi
chmod +x run_hadoop_lab.sh mapper_revenue.py reducer_revenue.py

# Chạy toàn bộ luồng HDFS -> MapReduce -> Xuất kết quả & Log
./run_hadoop_lab.sh
```

### Cách 2: Kiểm thử cục bộ bằng luồng pipe dòng lệnh
```bash
cat orders_2026_09.csv | python3 mapper_revenue.py | sort | python3 reducer_revenue.py
```

### Cách 3: Chạy trực tiếp qua Hadoop Streaming jar
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper_revenue.py,reducer_revenue.py \
    -mapper "python3 mapper_revenue.py" \
    -reducer "python3 reducer_revenue.py" \
    -input /user/labex/input/orders_2026_09.csv \
    -output /user/labex/output_revenue
```

---

## 📑 5. Báo cáo chi tiết

- [Báo cáo thực hành & Minh chứng kỹ thuật](BAO_CAO_THUC_HANH.md)
- [Báo cáo phân tích chuyên sâu 5 câu hỏi lý thuyết Hadoop MapReduce](BAO_CAO_THUC_HANH_HADOOP_MAPREDUCE.md)
