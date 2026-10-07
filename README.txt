BÀI THỰC HÀNH: PHÂN TÍCH ĐƠN HÀNG THƯƠNG MẠI ĐIỆN TỬ BẰNG HDFS VÀ MAPREDUCE
Học phần: IS330 - Dữ liệu Lớn (Big Data)
Trường Đại học Thăng Long (Thang Long University)
Sinh viên thực hiện: Nguyễn Văn Khải - MSSV: A47340

1. CẤU TRÚC THƯ MỤC NỘP BÀI (A47340_NguyenVanKhai_HadoopLab):
   - orders_2026_09.csv: Bộ dữ liệu giao dịch thương mại điện tử tháng 09/2026
   - mapper_revenue.py: Mã nguồn Mapper (Python 3) lọc giao dịch SUCCESS và xuất <category>\t<amount>
   - reducer_revenue.py: Mã nguồn Reducer (Python 3) gom nhóm và cộng dồn doanh thu theo category
   - run_hadoop_lab.sh: Kịch bản bash tự động hóa các thao tác HDFS và chạy Hadoop Streaming
   - part-00000: File kết quả đầu ra trực tiếp từ Hadoop MapReduce
   - result_revenue_by_category.txt: File kết quả tải từ HDFS về máy cục bộ
   - BAO_CAO_THUC_HANH_HADOOP_MAPREDUCE.md: Báo cáo kết quả và phân tích chi tiết

2. CÁCH CHẠY TRỰC TIẾP TRÊN MÔI TRƯỜNG HADOOP (LabEx / Linux):
   - Cấp quyền: chmod +x run_hadoop_lab.sh mapper_revenue.py reducer_revenue.py
   - Chạy toàn bộ luồng: ./run_hadoop_lab.sh
   - Hoặc chạy thử nghiệm cục bộ với Python pipe:
     cat orders_2026_09.csv | python3 mapper_revenue.py | sort | python3 reducer_revenue.py
