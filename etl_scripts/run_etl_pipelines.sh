#!/bin/bash

# run_etl_pipelines.sh

set -e  # Выход при первой ошибке

# Конфигурация
LOG_DIR="$(dirname "$0")"
LOG_FILE="$LOG_DIR/dwh_loader.log"
SCRIPTS_DIR="$(dirname "$0")"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Функция для логирования
log() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

# Функция для выполнения скрипта с проверкой
run_script() {
    local script_name=$1
    local script_path="$SCRIPTS_DIR/$script_name"
    
    log "Запуск скрипта: $script_name"
    
    if [ ! -f "$script_path" ]; then
        log "ОШИБКА: Скрипт $script_name не найден!"
        return 1
    fi
    
    # Запуск Python скрипта
    if python3 "$script_path" 2>&1 | tee -a "$LOG_FILE"; then
        log "УСПЕХ: Скрипт $script_name выполнен успешно"
        return 0
    else
        log "ОШИБКА: Скрипт $script_name завершился с ошибкой!"
        return 1
    fi
}

# Основной процесс
main() {
    log "========== Начало выполнения ETL пайплайна =========="
    
    # Шаг 1: Из источника в staging
    if run_script "from_source_to_staging.py"; then
        log "Переход к следующему шагу..."
        
        # Шаг 2: Из staging в DWH
        if run_script "from_staging_to_dwh.py"; then
            log "========== ETL пайплайн успешно завершен =========="
            exit 0
        else
            log "========== ОШИБКА на этапе Staging -> DWH =========="
            exit 1
        fi
    else
        log "========== ОШИБКА на этапе Source -> Staging =========="
        exit 1
    fi
}

# Запуск основного процесса
main
