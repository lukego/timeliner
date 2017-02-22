library(dplyr)
library(stringr)
library(bit64)

# Counter for automatically assigning process numbers when in doubt
count <- 0
counter <- function () { counter <<- count + 1; count }

# This special filename format is recognized:
#   .../[$group/]$process/engine/timeline
# ... and in this case the group and process names are used for the data.
# if group is not matched then it is left blank, if process is not matched then one is assigned.
read_timeline <- function(filename) {
  message("  reading ", filename)
  group <- str_match(filename, "([^/]+)/[^/]+/engine/timeline")[2]
  process <- str_match(filename, "([^/]+)/engine/timeline")[2]
  if (is.na(process)) { process <- paste("p", counter(), sep="") }
  message("    group=", group, " process=", process)
  timeline <- read_binary_timeline(filename)
  timeline$group <- as.factor(group)
  timeline$process <- as.factor(process)
  timeline$numa <- as.factor(timeline$numa)
  return(timeline)
}

read_binary_timeline <- function(filename) {
  f <- file(filename, "rb")
  # Read fields
  magic <- readBin(f, raw(), n=8, endian="little")
  version <- readBin(f, "integer", n=2, size=2, endian="little")
  log_bytes <- readBin(f, "integer", n=1, size=4, endian="little")
  strings_bytes <- readBin(f, "integer", n=1, size=4, endian="little")
  # Check compat
  if (!all(magic == c(0x01, 0x00, 0x1d, 0x44, 0x23, 0x72, 0xff, 0xa3))) {
    stop("bad magic number")
  }
  if (version[1] != 2) {
    stop("unrecognized major version")
  }
  # Read entries. These are 64-bit integers on disk but numeric (double-float)
  # in our data frame. (That's because dplyr doesn't support 64-bit integers
  # from the bit64 library.)
  seek(f, 64)
  # Load the values into two vectors: int64 (integer) and num (double-float).
  # This way we can access them in either format depending on our needs.
  int64 <- readBin(f, "double", n=log_bytes/8, size=8, endian="little")
  class(int64) <- "integer64"
  col0 <- seq(1, log_bytes/8, 64/8) # column 0 indexes
  message("    decoding entries")
  tl <- tibble(tsc = int64[col0],
               msgid = bitwAnd(as.integer(int64[col0+1]), 0xFFFF),
               core = bitwAnd(bitwShiftR(as.integer(int64[col0+1]), 16), 0xF),
               numa = bitwShiftR(as.integer(int64[col0+1]), 24),
               arg0 = int64[col0+2],
               arg1 = int64[col0+3],
               arg2 = int64[col0+4],
               arg3 = int64[col0+5],
               arg4 = int64[col0+6],
               arg5 = int64[col0+7])
  # Read strings
  message("    decoding string table")
  stringtable <- character(strings_bytes/16) # dense array
  start <- 64+log_bytes
  seek(f, start)
  repeat {
    id <- 1+(seek(f)-start)/16
    s <- readBin(f, "character")
    if (s == "") break;
    stringtable[id] <- s
    seek(f, ceiling(seek(f)/16) * 16) # seek to 16-byte alignment
  }
  messages <- tibble(msgid = 0:(length(stringtable)-1), message = stringtable) %>%
    filter(message != "") %>%
    mutate(summary = str_extract(message, "^[^\n]+"),
           level = as.integer(str_extract(summary, "^[0-9]")),
           event = gsub("^[0-9]\\|([^:]+):.*", "\\1", summary))
  tl <- left_join(tl, messages, by="msgid")
  # Calculate cycles
  message("    calculating cycle deltas")  
  # reference timestamp accumulator for update inside closure.
  # index is log level and value is reference timestamp for delta.
  ref <- as.numeric(rep(NA, 9))
  tscdelta <- function(level, time) {
    delta <- as.numeric(time) - ref[level]
    ref[1:level] <<- as.numeric(time)
    delta
  }
  tl$cycles <- mapply(tscdelta, tl$level, tl$tsc)
  # Convert from int64 to numeric for dplyr
  print("    converting int64 to numeric")
  convert <- c("tsc", "msgid", "core", "numa", "arg0", "arg1", "arg2", "arg3", "arg4", "arg5")
  tl[convert] <- lapply(tl[convert], as.numeric)
  return(tl)
}

read_timelines <- function(filenames) {
  return(do.call(rbind,lapply(filenames, read_timeline)))
}

# Efficiency terrain (overall)

prepare_efficiency_terrain <- function(data) {
  data %>% filter(grepl("^engine.breath_end$", event)) %>%
    transmute(packets=arg1, bpp=arg2, bits=arg1*arg2*8, bpc = bits/cycles) %>%
    na.omit() %>%
    group_by(floor(packets/20), floor(bpp/64)) %>% summarize(bpp = floor(first(bpp)/64)*64, packets=floor(first(packets)/20)*20, bpc = mean(bpc))
}

ggplot_efficiency_terrain <- function(data) {
  message("  plotting")
  ggplot(data, aes(y=packets, x=bpp)) + #, fill=bpc, color=bpc, z=bpc)) +
  geom_raster(aes(fill=bpc), interpolate=T) +
    stat_contour(aes(z=bpc), color="white", alpha=0.075) +
    scale_color_gradient(low="red", high="blue") +
    labs(title = "Map of 'efficiency terrain' in bits of traffic per CPU cycle (bpc)",
         x = "bytes per packet (average for breath)",
         y = "packets processed in breath (burst size)")
}

# Efficiency terrain (per app)

prepare_app_terrain <- function(data) {
  data %>% filter(grepl("^app.(pull|push)", event)) %>%
    rowwise() %>% mutate(tpackets = max(arg0, arg2), tbytes = max(arg1, arg3)) %>% ungroup() %>%
    mutate(packets = tpackets - lag(tpackets), bytes = tbytes - lag(tbytes), bpp = bytes/packets, bpc = bytes*8/cycles) %>%
    filter(grepl("^app.(pushed|pulled)", event))
}

ggplot_app_terrain <- function(data) {
  data <- data %>%
    group_by(event, floor(packets/20), floor(bpp/20)) %>%
    summarize(bpp = floor(first(bpp)/64)*64, packets=floor(first(packets)/20)*20, bpc = min(100, mean(bpc))) %>%
    ungroup()
  ggplot(data, aes(y=packets, x=bpp)) +
    geom_raster(aes(fill=bpc), interpolate=T) +
    facet_wrap(ncol=2, ~event) 
}

# KPIs per app

kpi_app <- function(data) {
  data %>% filter(grepl("^app.(pull|push)", event)) %>%
    rowwise() %>% mutate(tpackets = max(arg0, arg2), tbytes = max(arg1, arg3)) %>% ungroup() %>%
    mutate(packets = tpackets - lag(tpackets), bytes = tbytes - lag(tbytes),
           bpp = bytes/packets, cycles_per_packet = cycles/packets, cycles_per_byte = cycles/bytes) %>%
    filter(grepl("^app.(pushed|pulled)", event)) %>%
    na.omit() %>%
    group_by(event) %>% summarize(median_cycles_per_packet = round(median(cycles_per_packet), 1),
                                  median_cycles_per_byte = round(median(cycles_per_byte), 3),
                                  mean_bytes_per_packet = round(sum(bytes)/sum(packets), 0)) %>% ungroup()
}

# Graph: app cycles per packet

prepare_app_cycles_per_packet <- function(timeline) {
  message("  preparing data")
  timeline %>%
    filter(grepl("^app.(push|pull)", event)) %>%
    # Just the information needed for the graph
    mutate(what = gsub("^app.(pushed|pulled) app=(.*)$", "\\1 \\2", event),
           id = as.factor(paste(group, process)),
           packets = arg0-lag(arg0)+arg2-lag(arg2)) %>%
    # Just meaningful events
    filter(grepl("^app.(pushed|pulled)", event)) %>%
    filter(!is.na(cycles) & !is.na(packets) & packets>0)
}

ggplot_app_cycles_per_packet <- function(data) {
  message("  plotting")
  ggplot(data, aes(x=packets, y=cycles/packets, id)) +
    facet_wrap(~ what) +
    geom_point(alpha=0.025) +
#    geom_smooth(method=loess) +
    scale_y_continuous(limits = c(0, 1000), breaks=seq(0, 1000, 50)) +
    scale_x_continuous(limits = c(0, 512), breaks=seq(0, 512, 64)) +
    labs(title = "Packet processing cost (cycles/packet) breakdown",
         x = "number of packets processed together in a batch",
         y = "cycles per packet")
}

save <- function(filename) {
  message("  saving ", filename)
  ggsave(filename)
}

analyze_timelines <- function(filenames) {
  message("Reading timeline files")
  tl <- read_timelines(filenames)
  message("Creating app-wise cycles-per-packet graphs")
  appcyc <- prepare_app_cycles_per_packet(tl)
  plot <- ggplot_app_cycles_per_packet(appcyc)
  save("timeline-cycles_per_packet-by_process-per_app.png")
}

