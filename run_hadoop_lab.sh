#!/usr/bin/env bash
# ==============================================================================
# BÀI THỰC HÀNH: PHÂN TÍCH ĐƠN HÀNG THƯƠNG MẠI ĐIỆN TỬ BẰNG HDFS VÀ MAPREDUCE
# Học phần: IS330 - Dữ liệu Lớn | Trường Đại học Thăng Long (TLU)
# Tác giả: Nguyễn Văn Khải - MSSV A47340
# ==============================================================================

set -e # Dừng script nếu có lỗi phát sinh

echo "======================================================================"
echo ">>> BƯỚC 1: KIỂM TRA MÔI TRƯỜNG THỰC THI (HADOOP & PYTHON)"
echo "======================================================================"
hadoop version
python3 --version

echo ""
echo "======================================================================"
echo ">>> BƯỚC 2: CẤP QUYỀN THỰC THI CHO SCRIPT PYTHON"
echo "======================================================================"
chmod +x mapper_revenue.py reducer_revenue.py
ls -la mapper_revenue.py reducer_revenue.py

echo ""
echo "======================================================================"
echo ">>> BƯỚC 3: KHỞI TẠO CÂY THƯ MỤC LÀM VIỆC TRÊN HDFS"
echo "======================================================================"
hadoop fs -mkdir -p /user/student/ecommerce/input
hadoop fs -mkdir -p /user/student/ecommerce/output
hadoop fs -ls /user/student/ecommerce

echo ""
echo "======================================================================"
echo ">>> BƯỚC 4: ĐẨY FILE DỮ LIỆU LOCAL LÊN HDFS (HDFS PUT)"
echo "======================================================================"
hadoop fs -put -f orders_2026_09.csv /user/student/ecommerce/input/

echo ""
echo "======================================================================"
echo ">>> BƯỚC 5: KIỂM TRA VÀ XEM TRƯỚC DỮ LIỆU TRÊN HDFS (HDFS LS & HEAD)"
echo "======================================================================"
hadoop fs -ls /user/student/ecommerce/input
echo "--- 10 dòng đầu tiên của file trên HDFS ---"
hadoop fs -head /user/student/ecommerce/input/orders_2026_09.csv | head -n 10

echo ""
echo "======================================================================"
echo ">>> BƯỚC 6: DỌN DẸP THƯ MỤC OUTPUT CŨ (TRÁNH LỖI OUTPUT ALREADY EXISTS)"
echo "======================================================================"
hadoop fs -rm -r -f /user/student/ecommerce/output/revenue_by_category

echo ""
echo "======================================================================"
echo ">>> BƯỚC 7: XÁC ĐỊNH ĐƯỜNG DẪN HADOOP STREAMING JAR VÀ THỰC THI JOB"
echo "======================================================================"
# Tìm đường dẫn streaming jar tương thích với môi trường LabEx / Apache Hadoop
STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1)

if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar"
fi

echo "Sử dụng streaming jar: $STREAMING_JAR"

hadoop jar $STREAMING_JAR \
  -files mapper_revenue.py,reducer_revenue.py \
  -mapper mapper_revenue.py \
  -reducer reducer_revenue.py \
  -input /user/student/ecommerce/input/orders_2026_09.csv \
  -output /user/student/ecommerce/output/revenue_by_category

echo ""
echo "======================================================================"
echo ">>> BƯỚC 8: KIỂM TRA VÀ ĐỌC KẾT QUẢ ĐẦU RA TRÊN HDFS"
echo "======================================================================"
hadoop fs -ls /user/student/ecommerce/output/revenue_by_category
echo "--- Toàn bộ kết quả tính toán doanh thu (HDFS CAT) ---"
hadoop fs -cat /user/student/ecommerce/output/revenue_by_category/part-*

echo ""
echo "======================================================================"
echo ">>> BƯỚC 9: TẢI KẾT QUẢ VỀ MÁY CỤC BỘ (LOCAL FILE)"
echo "======================================================================"
hadoop fs -get -f /user/student/ecommerce/output/revenue_by_category/part-* ./result_revenue_by_category.txt
cp ./result_revenue_by_category.txt ./part-00000
echo "Nội dung file local result_revenue_by_category.txt:"
cat result_revenue_by_category.txt

echo ""
echo "======================================================================"
echo ">>> BƯỚC 10: XUẤT TOP 3 NHÓM SẢN PHẨM CÓ DOANH THU CAO NHẤT"
echo "======================================================================"
echo "Hạng | Nhóm sản phẩm (Category) | Tổng Doanh thu (VND)"
echo "------------------------------------------------------"
sort -k2 -nr result_revenue_by_category.txt | head -3 | awk -F'\t' '{printf "%-2d   | %-24s | %15s VND\n", NR, $1, $2}'

echo ""
echo ">>> HOÀN THÀNH TOÀN BỘ QUY TRÌNH MAPREDUCE TRÊN HADOOP!"
