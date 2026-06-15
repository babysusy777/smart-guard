CONTIKI_PROJECT = patient_node
all: $(CONTIKI_PROJECT)

CONTIKI = ../..
include $(CONTIKI)/Makefile.dir-variables

MODULES += $(CONTIKI_NG_APP_LAYER_DIR)/coap
MODULES += $(CONTIKI_NG_APP_LAYER_DIR)/mqtt
MODULES_REL += ./resources

PROJECT_SOURCEFILES += patient_data_generator.c
INC += ./contiki_generated

MODULES_REL += /home/vitto/.local/lib/python3.12/site-packages/emlearn
TARGET_LIBFILES += -lm
INC += /home/vitto/.local/lib/python3.12/site-packages/emlearn

include $(CONTIKI)/Makefile.include