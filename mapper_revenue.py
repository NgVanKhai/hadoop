#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Học phần: IS330 - Dữ liệu Lớn (Big Data) - Thăng Long University
Bài thực hành: Phân tích đơn hàng thương mại điện tử bằng HDFS và MapReduce
Script: mapper_revenue.py
Chức năng: Đọc từng dòng dữ liệu từ sys.stdin, lọc các giao dịch có trạng thái SUCCESS,
           và phát sinh cặp Key-Value dạng <category>\t<amount>.
"""

import sys

def main():
    for line in sys.stdin:
        # Loại bỏ khoảng trắng thừa đầu/cuối dòng
        line = line.strip()
        if not line:
            continue

        # Tách các trường dữ liệu theo dấu phẩy
        parts = line.split(',')

        # Bỏ qua dòng tiêu đề (header) nếu gặp
        if parts[0].strip().lower() == 'transaction_id':
            continue

        # Kiểm tra tính hợp lệ của số lượng cột
        # Schema: transaction_id, order_date, order_time, province, category, channel, quantity, unit_price, amount, status, shipping_days
        if len(parts) < 10:
            continue

        try:
            category = parts[4].strip()
            amount_str = parts[8].strip()
            status = parts[9].strip().upper()

            # Logic nghiệp vụ: Chỉ ghi nhận doanh thu cho các giao dịch thành công (SUCCESS)
            if status == "SUCCESS":
                amount = float(amount_str)
                # Xuất ra chuẩn Hadoop Streaming: Key [TAB] Value
                # Ép kiểu int nếu amount là số nguyên để kết quả gọn gàng
                print(f"{category}\t{int(amount) if amount.is_integer() else amount}")
        except (ValueError, IndexError):
            # Bỏ qua các dòng lỗi định dạng số hoặc thiếu trường dữ liệu
            continue

if __name__ == "__main__":
    main()
