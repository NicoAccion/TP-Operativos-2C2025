#include "configs.h"

t_config* iniciar_config (char* path){

    t_config* nuevo_config = config_create(path);

	if (nuevo_config == NULL) {

		abort();
	}

	return nuevo_config;
}

int cargar_variable_int(t_config* config, char* nombre){
	if (config_has_property(config, nombre)){
		return config_get_int_value(config, nombre);
	}
	return -1;
}

char* cargar_variable_string(t_config* config, char* nombre){
	if (config_has_property(config, nombre)){
		return config_get_string_value(config, nombre);
	}
	return NULL;
}

double cargar_variable_double(t_config* config, char* nombre){
	if (config_has_property(config, nombre)){
		return config_get_double_value(config, nombre);
	}
	return -1;
}

bool cargar_variable_bool(t_config* config, char* nombre) {
    
    // 1. Verificar si la propiedad existe
    if (!config_has_property(config, nombre)) {
        fprintf(stderr, "ADVERTENCIA: La propiedad %s no existe en el config. Asumiendo 'false'.\n", nombre);
        return false; 
    }

    // 2. Obtener el valor como STRING
    char* valor_str = config_get_string_value(config, nombre);
    
    // 3. Comparar la cadena de texto de forma insensible a mayúsculas
    if (valor_str != NULL && strcasecmp(valor_str, "TRUE") == 0) {
        return true;
	}
    // Cualquier otro valor (NULL, "FALSE", "0", etc.) es falso
    return false;
}