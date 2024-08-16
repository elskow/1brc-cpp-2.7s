#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <sstream>

struct locationData {
    uint64_t count = 0;
    float total = 0;
    float temp_min = 99;
    float temp_max = -99;
};

/**
 * Parse a float number from a string.
 * @param str The string to parse.
 * @param len The length of the string.
 * @return The parsed float number.
 */
float parse_float(const char* str, size_t len) {
    float result = 0.0f;
    bool negative = false;
    size_t i = 0;

    // Skip leading whitespaces
    if (str[i] == '-') {
        negative = true;
        i++;
    }

    // Parse the integer part
    while (i < len && str[i] != '.') {
        result = result * 10.0f + static_cast<float>(str[i] - '0');
        i++;
    }

    // Parse the fraction part
    if (i < len && str[i] == '.') {
        i++;
        float fraction = 1.0f;
        while (i < len) {
            fraction *= 0.1f;
            result += static_cast<float>(str[i] - '0') * fraction;
            i++;
        }
    }

    return negative ? -result : result;
}

/**
 * Process a line of data and update the location data.
 * @param line The line to process.
 * @param loc_data The location data to update.
 */
void process_line(std::string_view line, std::unordered_map<std::string, locationData>& loc_data) {
    size_t delimiter_pos = line.find(';'); // Find the first delimiter
    if (delimiter_pos == std::string_view::npos) return; // Skip invalid lines

    std::string location = std::string(line.substr(0, delimiter_pos)); // Extract the location
    float temperature_num = parse_float(line.data() + delimiter_pos + 1, line.size() - delimiter_pos - 1); // Extract the temperature

    auto& loc_value = loc_data[location];
    loc_value.count++;
    loc_value.total += temperature_num;
    if (loc_value.temp_max < temperature_num) {
        loc_value.temp_max = temperature_num; // Update the maximum temperature
    }
    if (loc_value.temp_min > temperature_num) {
        loc_value.temp_min = temperature_num; // Update the minimum temperature
    }
}

/**
 * Process a chunk of data and update the location data.
 * @param data The data to process.
 * @param start_pos The start position of the chunk.
 * @param end The end position of the chunk.
 * @param loc_data The location data to update.
 */
void process_chunk(const char* data, size_t start_pos, size_t end, std::unordered_map<std::string, locationData>& loc_data) {
    size_t i = start_pos;
    while (i < end) {
        const char* line_start = data + i;
        while (i < end && data[i] != '\n') {
            ++i;
        }
        process_line(std::string_view(line_start, data + i - line_start), loc_data);
        ++i; // Skip the newline character
    }
}

int main(int argc, char* argv[]) {
    std::ios::sync_with_stdio(false); // Disable synchronization with C I/O
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <file_path>\n";
        return 1;
    }

    const char* file_path = argv[1];
    auto start_time = std::chrono::system_clock::now();

    int fd = open(file_path, O_RDONLY);
    if (fd == -1) {
        std::cerr << "Can't open input file: " << file_path << "\n";
        return 1;
    }

    struct stat sb = {};
    if (fstat(fd, &sb) == -1) {
        std::cerr << "Can't get file size for: " << file_path << "\n";
        close(fd);
        return 1;
    }

    char* file_data = static_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0)); // Map the file to memory
    if (file_data == MAP_FAILED) {
        std::cerr << "Can't map file: " << file_path << "\n";
        close(fd);
        return 1;
    }

    std::unordered_map<std::string, locationData> loc_data;
    std::mutex loc_data_mutex;

    size_t num_threads = std::thread::hardware_concurrency(); // Get the number of hardware threadss
    size_t chunk_size = sb.st_size / num_threads; // Calculate the chunk size
    std::vector<std::thread> threads;

    // Create threads to process chunks of data
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start_pos = i * chunk_size;
        size_t end = (i == num_threads - 1) ? sb.st_size : (i + 1) * chunk_size;

        threads.emplace_back([&, start_pos, end]() {
            std::unordered_map<std::string, locationData> local_loc_data;
            process_chunk(file_data, start_pos, end, local_loc_data);

            std::lock_guard<std::mutex> lock(loc_data_mutex);
            for (const auto& [key, value] : local_loc_data) {
                auto& loc_value = loc_data[key];
                loc_value.count += value.count;
                loc_value.total += value.total;
                if (loc_value.temp_max < value.temp_max) {
                    loc_value.temp_max = value.temp_max;
                }
                if (loc_value.temp_min > value.temp_min) {
                    loc_value.temp_min = value.temp_min;
                }
            }
        });
    }

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    // Unmap the file and close the file descriptor
    munmap(file_data, sb.st_size);
    close(fd);

    auto end_time = std::chrono::system_clock::now();
    typedef std::chrono::duration<float> fsec;
    fsec fs = end_time - start_time;

    std::ostringstream output;
    output << "Location;Count;Average;Min;Max\n";
    for (const auto& [key, value] : loc_data) {
        output << key << ';' << value.count << ';' << value.total / value.count << ';' << value.temp_min << ';' << value
                .temp_max << '\n';
    }
    output << "\n" << "Time: ";
    output << fs.count() << "s\n";
    std::cout << output.str();
    return 0;
}