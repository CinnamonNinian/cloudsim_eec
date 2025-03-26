# Compiler
CXX = g++
# Compiler flags
CXXFLAGS = -Wall -std=c++17
# Include directories
INCLUDES = -I.

# Source files
SRC = Init.cpp Machine.cpp main.cpp Scheduler.cpp Simulator.cpp Task.cpp VM.cpp

# Object files
OBJ = $(SRC:.cpp=.o)

# Executable
TARGET = simulator 

# Default target
all: $(TARGET)

# Default target
scheduler: $(OBJ)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o scheduler $(OBJ)

# Build target
$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -g -o $(TARGET) $(OBJ)

# Compile source files into object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Made with chatGPT
	# Run the simulator executable
run: $(TARGET)
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make run FILE=<input_file_name> [TAG=<optional_tag>]"; \
		exit 1; \
	fi
	@if [ -z "$(V)" ]; then \
		./$(TARGET) input_files/$(FILE) > output_$(FILE).txt; \
	else \
		./$(TARGET) -v $(V) input_files/$(FILE) > output_$(FILE).txt; \
	fi


# Clean up build files
clean:
	rm -f $(OBJ) $(TARGET)
