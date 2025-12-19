-- 1. Таблица пользователей
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    role VARCHAR(20) NOT NULL CHECK (role IN ('student', 'teacher')),
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone_number VARCHAR(20),
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Таблица учеников
CREATE TABLE students (
    student_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL UNIQUE REFERENCES users(user_id) ON DELETE CASCADE,
    current_grade INTEGER CHECK (current_grade BETWEEN 1 AND 11),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Таблица преподавателей
CREATE TABLE teachers (
    teacher_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL UNIQUE REFERENCES users(user_id) ON DELETE CASCADE,
    hourly_rate DECIMAL(10,2) NOT NULL CHECK (hourly_rate > 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 4. Таблица предметов
CREATE TABLE subjects (
    subject_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 5. Таблица связей преподаватель-предмет
CREATE TABLE teacher_subjects (
    teacher_subject_id SERIAL PRIMARY KEY,
    teacher_id INTEGER NOT NULL REFERENCES teachers(teacher_id) ON DELETE CASCADE,
    subject_id INTEGER NOT NULL REFERENCES subjects(subject_id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(teacher_id, subject_id)
);

-- 6. Таблица пакетов уроков
CREATE TABLE lesson_packs (
    pack_id SERIAL PRIMARY KEY,
    lessons_count INTEGER NOT NULL CHECK (lessons_count > 0),
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    duration_days INTEGER NOT NULL CHECK (duration_days > 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 7. Таблица приобретенных пакетов студентов
CREATE TABLE students_purchases (
    purchase_id SERIAL PRIMARY KEY,
    student_id INTEGER NOT NULL REFERENCES students(student_id) ON DELETE CASCADE,
    pack_id INTEGER NOT NULL REFERENCES lesson_packs(pack_id),
    purchase_price DECIMAL(10,2) NOT NULL CHECK (purchase_price >= 0),
    lessons_total INTEGER NOT NULL CHECK (lessons_total > 0),
    lessons_remaining INTEGER NOT NULL CHECK (lessons_remaining >= 0 AND lessons_remaining <= lessons_total),
    purchase_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'expired', 'used_up')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 8. Таблица уроков
CREATE TABLE lessons (
    lesson_id SERIAL PRIMARY KEY,
    student_id INTEGER NOT NULL REFERENCES students(student_id) ON DELETE CASCADE,
    purchase_id INTEGER NOT NULL REFERENCES students_purchases(purchase_id),
    teacher_subject_id INTEGER NOT NULL REFERENCES teacher_subjects(teacher_subject_id),
    scheduled_start_time TIMESTAMP NOT NULL,
    scheduled_end_time TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'completed', 'student_absent', 'teacher_absent', 'canceled')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (scheduled_end_time > scheduled_start_time)
);

-- 9. Таблица домашних заданий
CREATE TABLE homeworks (
    homework_id SERIAL PRIMARY KEY,
    lesson_id INTEGER NOT NULL UNIQUE REFERENCES lessons(lesson_id) ON DELETE CASCADE,
    submitted_at TIMESTAMP,
    score INTEGER CHECK (score BETWEEN 0 AND 100),
    deadline DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'assigned' CHECK (status IN ('assigned', 'submitted', 'graded', 'overdue')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
