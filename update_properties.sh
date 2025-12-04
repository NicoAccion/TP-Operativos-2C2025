#!/usr/bin/env bash

# Valido que se pasen los argumentos correctos
if [ $# -ne 2 ]; then
  echo "Uso: $0 clave nuevo_valor"
  echo "Ejemplo: $0 ip_memory 192.168.0.32"
  exit 1
fi

CLAVE="$1"
NUEVO_VALOR="$2"

# Directorios donde buscar
DIRS=(master worker storage query_control)

for dir in "${DIRS[@]}"; do
  CONFIG_PATH="$dir/configs"
  
  # Verifico si el directorio existe
  if [ -d "$CONFIG_PATH" ]; then
    # Itero sobre los archivos .config
    for archivo in "$CONFIG_PATH"/*.config; do
      if [ -f "$archivo" ]; then
        echo "Modificando $archivo ..."
        
        sed "s|^$CLAVE=.*|$CLAVE=$NUEVO_VALOR|" "$archivo" > "${archivo}.tmp" && mv "${archivo}.tmp" "$archivo"
      fi
    done
  fi
done

echo "Modificación completada."
