.PHONY: all clean

TOP=.
BUILD_DIR=./build

CC= gcc
CC_FLAGS= -g3 -O2 -rdynamic -Wall -fPIC -shared -Wextra

all: build
build:
	-mkdir -p $(BUILD_DIR)

CLUALIB=crypt cmsgpack rc4
CLUALIB_TARGET=$(patsubst %, $(BUILD_DIR)/%.so, $(CLUALIB))
all: $(CLUALIB_TARGET)

lua-cmsgpack/lua_cmsgpack.c:
	git submodule update --init lua-cmsgpack

$(BUILD_DIR)/cmsgpack.so: lua-cmsgpack/lua_cmsgpack.c
	$(CC) $(CC_FLAGS) $^ -o $@

crypto/crypt.c:
	git submodule update --init crypto

$(BUILD_DIR)/crypt.so: crypto/lsha1.c crypto/crypt.c
	$(CC) $(CC_FLAGS) $^ -o $@

rc4/rc4.c:
	git submodule update --init rc4

$(BUILD_DIR)/rc4.so: rc4/rc4.c rc4/luabinding.c
	$(CC) $(CC_FLAGS) $^ -o $@


clean:
	rm -rf $(BUILD_DIR)
