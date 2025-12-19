-- ГОТОВО (СТАТИЧЕСКАЯ)
CREATE TABLE Dim_Date
(
    date_key      UInt32,
    full_date     Date,         
    year          UInt16,        
    quarter       UInt8,       
    month         UInt8,      
    month_name    String,       
    week_of_year  UInt8,        
    day_of_week   UInt8
)
ENGINE = MergeTree()
ORDER BY date_key;

INSERT INTO Dim_Date
SELECT
    toYYYYMMDD(d) AS date_key,
    d AS full_date,
    toYear(d) AS year,
    toQuarter(d) AS quarter,
    toMonth(d) AS month,
    monthName(d) AS month_name,
    toWeek(d) AS week_of_year,
    dayOfWeek(d) AS day_of_week
FROM (
    SELECT 
        toDate('2023-01-01') + number AS d
    FROM numbers(
        toUInt64(
            toDate('2027-12-31') - toDate('2023-01-01') + 1
        )
    )
);

-- ГОТОВО (ПЕРЕЗАПИСЫВАЕМ КАЖДЫЙ РАЗ)
CREATE TABLE Dim_Subject
(
    subject_sk     UInt32,
    subject_id_nk UInt32,
    subject_name   String,
)
ENGINE = MergeTree()
ORDER BY (subject_id_nk, subject_sk);


-- ГОТОВО
CREATE TABLE Fact_Homeworks
(
    homework_fact_id     UInt64,
    date_assigned_key    UInt32,
    date_deadline_key    UInt32,
    date_submitted_key   Nullable(UInt32),
    student_sk           UInt32,
    subject_sk           UInt32,
    homework_id_nk       UInt32,
    score                Nullable(UInt8),
    homework_status      String,
    updated_at           DateTime DEFAULT now(),
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (homework_id_nk, student_sk, date_assigned_key);

-- ГОТОВО
CREATE TABLE Fact_Lessons
(
    lesson_fact_id       UInt64,
    date_key             UInt32,
    time_start           DateTime,
    student_sk           UInt32,
    teacher_sk           UInt32,
    subject_sk           UInt32,
    lesson_id_nk         UInt32,
    duration_minutes     UInt16,
    teacher_cost_amount  Decimal(10, 2),
    lesson_status        String,
    updated_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (lesson_id_nk, date_key, teacher_sk, student_sk);

-- ГОТОВО
CREATE TABLE Fact_Sales
(
    sales_id          UInt64,
    date_key          UInt32,
    student_sk        UInt32,
    purchase_id_nk    UInt32,
    purchase_amount   Decimal(10, 2),
    lessons_total     UInt16,
    purchase_status   String,
    updated_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (purchase_id_nk, student_sk, date_key);

-- ГОТОВО (SCD2 на уровне питон скрипта)
CREATE TABLE Dim_Student
(
    student_sk     UInt64,
    student_id_nk  UInt64,
    user_id_nk     UInt64,
    full_name      String,
    phone_number   String,
    current_grade  String,
    status         String,
    valid_from     Date,
    valid_to       Date DEFAULT toDate('2099-12-31'),
    is_current     UInt8 DEFAULT 1,
    updated_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY student_id_nk;

-- ГОТОВО (SCD2 на уровне питон скрипта)
CREATE TABLE Dim_Teacher
(
    teacher_sk      UInt64,
    teacher_id_nk   UInt64,
    full_name       String,
    phone_number    String,
    hourly_rate     Decimal(10, 2),
    status          String DEFAULT 'active',
    valid_from      Date,
    valid_to        Date DEFAULT toDate('2099-12-31'),
    is_current      UInt8 DEFAULT 1,
    updated_at      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (teacher_id_nk, valid_from);
