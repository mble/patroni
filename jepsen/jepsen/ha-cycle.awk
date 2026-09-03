BEGIN {
    HOURS_PER_DAY = 24
    MINUTES_PER_HOUR = 60
    SECONDS_PER_MINUTE = 60
    MILLISECONDS_PER_SECOND = 1000
}

function milliseconds(value, parts) {
    split(value, parts, /[:,]/)
    return (((parts[1] * MINUTES_PER_HOUR + parts[2]) * SECONDS_PER_MINUTE + parts[3]) \
        * MILLISECONDS_PER_SECOND + parts[4])
}

/ INFO: Lock owner:/ {
    started = milliseconds($2)
    next
}

/ INFO: no action\./ && started {
    finished = milliseconds($2)
    if (finished < started) {
        finished += HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * MILLISECONDS_PER_SECOND
    }
    print finished - started
    started = 0
}
