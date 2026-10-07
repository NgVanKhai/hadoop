#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Học phần: IS330 - Dữ liệu Lớn (Big Data) - Thăng Long University
Bài thực hành: Phân tích đơn hàng thương mại điện tử bằng HDFS và MapReduce
Script: reducer_revenue.py
Chức năng: Nhận dòng dữ liệu đã được Shuffle and Sort theo Key (category) từ sys.stdin,
           thực hiện gom nhóm và cộng dồn doanh thu (amount) theo từng category.
"""

import sys

def main():
    current_category = None
    total_amount = 0.0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Hadoop Streaming phân cách Key và Value mặc định bằng dấu TAB (\t)
        parts = line.split('\t')
        if len(parts) != 2:
            continue

        category, amount_str = parts[0].strip(), parts[1].strip()

        try:
            amount = float(amount_str)
        except ValueError:
            # Bỏ qua nếu giá trị amount không thể chuyển đổi thành số
            continue

        # Nếu category hiện tại trùng với category đang xử lý -> cộng dồn
        if current_category == category:
            total_amount += amount
        else:
            # Khi đổi sang category mới, in kết quả của category trước đó (nếu có)
            if current_category is not None:
                # In ra định dạng <category>\t<total_revenue>
                formatted_total = int(total_amount) if total_amount.is_integer() else total_amount
                print(f"{current_category}\t{formatted_total}")
            
            # Cập nhật category mới và khởi tạo lại tổng doanh thu
            current_category = category
            total_amount = amount

    # Đừng quên in kết quả của category cuối cùng sau khi kết thúc luồng sys.stdin
    if current_category is not None:
        formatted_total = int(total_amount) if total_amount.is_integer() else total_amount
        print(f"{current_category}\t{formatted_total}")

if __name__ == "__main__":
    main()
