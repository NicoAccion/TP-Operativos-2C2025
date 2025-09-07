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