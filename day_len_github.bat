@echo off
chcp 65001 >nul
echo ========================================================
echo     TỰ ĐỘNG ĐẨY BÀI THỰC HÀNH HADOOP LÊN GITHUB
echo     Sinh viên: Nguyễn Văn Khải - MSSV: A47340
echo     Repo: https://github.com/NgVanKhai/hadoop
echo ========================================================

:: 0. Tự động kiểm tra và cấu hình đường dẫn Git
where git >nul 2>&1
if errorlevel 1 (
    if exist "C:\Program Files\Git\cmd\git.exe" (
        set "PATH=C:\Program Files\Git\cmd;%PATH%"
    ) else if exist "C:\Program Files\Microsoft Visual Studio\18\Community\Common7\IDE\CommonExtensions\Microsoft\TeamFoundation\Team Explorer\Git\cmd\git.exe" (
        set "PATH=C:\Program Files\Microsoft Visual Studio\18\Community\Common7\IDE\CommonExtensions\Microsoft\TeamFoundation\Team Explorer\Git\cmd;%PATH%"
    ) else (
        echo [!] Khong tim thay Git! Vui long cai dat Git for Windows.
        pause
        exit /b 1
    )
)

:: 1. Tự động đổi tên thư mục bài làm sang đúng tên của bạn nếu còn để tên mẫu
cd /d "d:\A47340"
if exist "A47340_HoTen_HadoopLab" (
    if not exist "A47340_NguyenVanKhai_HadoopLab" (
        echo [*] Dang doi ten thu muc sang A47340_NguyenVanKhai_HadoopLab...
        ren "A47340_HoTen_HadoopLab" "A47340_NguyenVanKhai_HadoopLab"
    )
)

:: 2. Di chuyển vào thư mục bài làm chính thức
if exist "A47340_NguyenVanKhai_HadoopLab" (
    cd /d "d:\A47340\A47340_NguyenVanKhai_HadoopLab"
) else (
    echo [!] Khong tim thay thu muc bai lam tai d:\A47340\A47340_NguyenVanKhai_HadoopLab!
    pause
    exit /b 1
)

:: 3. Khởi tạo Git nếu chưa có
if not exist ".git" (
    echo [*] Khoi tao Git repository...
    git init
)

:: 4. Thiết lập tên và email tác giả commit (tránh lỗi Author unknown)
git config user.name "Nguyen Van Khai"
git config user.email "ngvankhai@users.noreply.github.com"

:: 5. Đổi tên nhánh sang main
echo [*] Thiet lap branch main...
git branch -M main

:: 6. Thêm remote GitHub
git remote remove origin 2>nul
git remote add origin https://github.com/NgVanKhai/hadoop.git

:: 7. Thêm tất cả các file vào Git
echo [*] Dang chuan bi cac file (git add)...
git add .

:: 8. Tạo commit nếu có thay đổi
echo [*] Dang tao commit...
git commit -m "Nop bai thuc hanh Hadoop MapReduce IS330 - Nguyen Van Khai - A47340" 2>nul
if errorlevel 1 (
    echo [*] Khong co thay doi moi can commit hoac da commit truoc do.
)

:: 9. Đẩy code lên GitHub
echo [*] Dang day code len GitHub (Neu hien cua so dang nhap, vui long chon dang nhap bang trinh duyet)...
git push -u origin main --force

if errorlevel 1 (
    echo.
    echo ========================================================
    echo [CANH BAO] Push that bai! 
    echo Ly do thuong gap: GitHub yeu cau xac thuc tai khoan.
    echo Cach xu ly:
    echo 1. Trinh duyet se hien cua so 'Git Credential Manager' de dang nhap GitHub.
    echo 2. Hoac su dung Personal Access Token (PAT) theo huong dan:
    echo    git push https://<TOKEN>@github.com/NgVanKhai/hadoop.git main --force
    echo ========================================================
) else (
    echo.
    echo ========================================================
    echo [THANH CONG] Toan bo bai lam da duoc day len GitHub!
    echo Xem tai: https://github.com/NgVanKhai/hadoop
    echo ========================================================
)

pause
