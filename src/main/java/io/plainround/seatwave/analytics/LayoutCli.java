package io.plainround.seatwave.analytics;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One aggregate query, "confirmed tickets per show per minute", over the same {@code fact_bookings}
 * rows in three layouts: rows on 8 kB pages as PostgreSQL stores them (Module 14), plain columns,
 * and encoded columns. The row pages are sized, not written; the columns are really encoded into
 * bytes and the query is answered by decoding them, and both answers are compared.
 */
final class LayoutCli {

  private static final int PAGE_BYTES = 8_192; // Module 14: PostgreSQL's page
  private static final int PAGE_HEADER_BYTES = 24;
  private static final int LINE_POINTER_BYTES = 4;
  private static final int ROW_HEADER_BYTES = 24; // 23 bytes, aligned to 8
  private static final int MAX_ALIGN = 8;
  private static final int UUID_BYTES = 16;
  private static final int BIGINT_BYTES = 8;
  private static final int INTEGER_BYTES = 4;
  private static final int SMALLINT_BYTES = 2;
  private static final int EIGHT_BYTE_COLUMNS =
      4; // event_id, customer_id, confirmed_at, status_changed_at
  private static final int SHORT_VARLENA_HEADER_BYTES = 1;
  private static final int BITS_PER_BYTE = 8;
  private static final int SEVEN_BITS = 0x7f;
  private static final int CONTINUATION_BIT = 0x80;
  private static final int VARINT_SHIFT = 7;
  private static final int MAX_DICTIONARY_CODES = 256;
  private static final Duration MINUTE = Duration.ofMinutes(1);
  private static final double MEGABYTE = 1_000_000.0;

  private LayoutCli() {}

  public static void main(String[] arguments) {
    OnSaleGenerator.Settings sale = OnSaleGenerator.Settings.nationalOnSale();
    WarehouseTable table = new WarehouseTable();
    new NightlyBatch()
        .load(LocalDate.of(2026, 11, 20), new OnSaleGenerator(sale).generate(), table);
    // A warehouse load appends in time order, so the columns are sorted by confirmed_at.
    List<WarehouseTable.FactBooking> rows = new ArrayList<>(table.rows());
    rows.sort(
        Comparator.comparing(WarehouseTable.FactBooking::confirmedAt)
            .thenComparing(WarehouseTable.FactBooking::bookingId));

    long rowBytesRead = rowStorePagesRead(rows) * PAGE_BYTES;
    long plainBytesRead = (long) rows.size() * (BIGINT_BYTES + BIGINT_BYTES + SMALLINT_BYTES);
    byte[] times = encodeTimes(rows);
    byte[] showCodes = new byte[rows.size()];
    long[] dictionary = encodeShows(rows, showCodes);
    byte[] confirmedBits = encodeStatus(rows);
    long encodedBytesRead =
        (long) times.length
            + showCodes.length
            + dictionary.length * BIGINT_BYTES
            + confirmedBits.length;

    Map<MinuteWindows.WindowKey, Long> fromRows = answerFromRows(rows);
    Map<MinuteWindows.WindowKey, Long> fromColumns =
        answerFromColumns(times, showCodes, dictionary, confirmedBits, rows.size());
    System.out.println(
        String.format(
            Locale.ROOT,
            "fact_bookings: %,d rows; query: confirmed tickets per show per minute (reads event_id,"
                + " confirmed_at, status)",
            rows.size()));
    System.out.println(
        String.format(
            Locale.ROOT,
            "  rows on 8 kB pages:   %,d pages, %,d bytes read (%.1f MB): every column of every"
                + " row",
            rowBytesRead / PAGE_BYTES,
            rowBytesRead,
            rowBytesRead / MEGABYTE));
    System.out.println(
        String.format(
            Locale.ROOT,
            "  plain columns:        %,d bytes read (%.1f MB): 8 + 8 + 2 bytes a row, three columns"
                + " only",
            plainBytesRead,
            plainBytesRead / MEGABYTE));
    System.out.println(
        String.format(
            Locale.ROOT,
            "  encoded columns:      %,d bytes read (%.2f MB): time deltas %,d, show codes %,d,"
                + " dictionary %,d, status bits %,d",
            encodedBytesRead,
            encodedBytesRead / MEGABYTE,
            times.length,
            showCodes.length,
            dictionary.length * BIGINT_BYTES,
            confirmedBits.length));
    System.out.println(
        String.format(
            Locale.ROOT,
            "  row store reads %.1f times the plain columns and %.1f times the encoded ones",
            (double) rowBytesRead / plainBytesRead,
            (double) rowBytesRead / encodedBytesRead));
    System.out.println(
        String.format(
            Locale.ROOT,
            "  both layouts give the same answer: %s (%,d windows, %,d confirmed)",
            fromRows.equals(fromColumns),
            fromRows.size(),
            fromRows.values().stream().mapToLong(Long::longValue).sum()));
  }

  /**
   * Pages a full scan reads: rows packed onto 8 kB pages in load order, as a heap file holds them.
   */
  private static long rowStorePagesRead(List<WarehouseTable.FactBooking> rows) {
    long pages = 1;
    int freeBytes = PAGE_BYTES - PAGE_HEADER_BYTES;
    for (WarehouseTable.FactBooking row : rows) {
      int needed = rowBytes(row) + LINE_POINTER_BYTES;
      if (needed > freeBytes) {
        pages++;
        freeBytes = PAGE_BYTES - PAGE_HEADER_BYTES;
      }
      freeBytes -= needed;
    }
    return pages;
  }

  /** One row's bytes in column order: uuid, five 8-byte values, version, status, seat label. */
  private static int rowBytes(WarehouseTable.FactBooking row) {
    int offset = ROW_HEADER_BYTES + UUID_BYTES;
    offset = align(offset, BIGINT_BYTES) + BIGINT_BYTES * EIGHT_BYTE_COLUMNS;
    offset = align(offset, INTEGER_BYTES) + INTEGER_BYTES; // booking_version
    offset = align(offset, SMALLINT_BYTES) + SMALLINT_BYTES; // status as a smallint code
    offset += SHORT_VARLENA_HEADER_BYTES + row.seatLabel().getBytes(StandardCharsets.UTF_8).length;
    return align(offset, MAX_ALIGN);
  }

  private static int align(int offset, int alignment) {
    return (offset + alignment - 1) / alignment * alignment;
  }

  /** confirmed_at as the difference in milliseconds from the previous row, 7 bits a byte. */
  private static byte[] encodeTimes(List<WarehouseTable.FactBooking> rows) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    long previousMillis = 0;
    for (WarehouseTable.FactBooking row : rows) {
      long millis = row.confirmedAt().toEpochMilli();
      long delta = millis - previousMillis; // sorted, so never negative
      previousMillis = millis;
      while (delta >= CONTINUATION_BIT) {
        bytes.write((int) (delta & SEVEN_BITS) | CONTINUATION_BIT);
        delta >>>= VARINT_SHIFT;
      }
      bytes.write((int) delta);
    }
    return bytes.toByteArray();
  }

  /** event_id as a one-byte code into a dictionary of the distinct shows. */
  private static long[] encodeShows(List<WarehouseTable.FactBooking> rows, byte[] codes) {
    Map<Long, Integer> codeOf = new HashMap<>();
    List<Long> dictionary = new ArrayList<>();
    for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
      long eventId = rows.get(rowIndex).eventId();
      Integer code = codeOf.get(eventId);
      if (code == null) {
        if (dictionary.size() == MAX_DICTIONARY_CODES) {
          throw new IllegalStateException("more than 256 shows: a one-byte code cannot hold them");
        }
        code = dictionary.size();
        codeOf.put(eventId, code);
        dictionary.add(eventId);
      }
      codes[rowIndex] = (byte) code.intValue();
    }
    return dictionary.stream().mapToLong(Long::longValue).toArray();
  }

  /** status as one bit a row: 1 for confirmed. */
  private static byte[] encodeStatus(List<WarehouseTable.FactBooking> rows) {
    byte[] bits = new byte[(rows.size() + BITS_PER_BYTE - 1) / BITS_PER_BYTE];
    for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
      if (rows.get(rowIndex).status() == WarehouseTable.Status.CONFIRMED) {
        bits[rowIndex / BITS_PER_BYTE] |= (byte) (1 << (rowIndex % BITS_PER_BYTE));
      }
    }
    return bits;
  }

  private static Map<MinuteWindows.WindowKey, Long> answerFromRows(
      List<WarehouseTable.FactBooking> rows) {
    Map<MinuteWindows.WindowKey, Long> answer = new HashMap<>();
    for (WarehouseTable.FactBooking row : rows) {
      if (row.status() == WarehouseTable.Status.CONFIRMED) {
        answer.merge(
            new MinuteWindows.WindowKey(row.eventId(), minuteOf(row.confirmedAt().toEpochMilli())),
            1L,
            Long::sum);
      }
    }
    return answer;
  }

  private static Map<MinuteWindows.WindowKey, Long> answerFromColumns(
      byte[] times, byte[] showCodes, long[] dictionary, byte[] confirmedBits, int rowCount) {
    Map<MinuteWindows.WindowKey, Long> answer = new HashMap<>();
    int position = 0;
    long millis = 0;
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      long delta = 0;
      int shift = 0;
      int next;
      do {
        next = times[position++];
        delta |= (long) (next & SEVEN_BITS) << shift;
        shift += VARINT_SHIFT;
      } while ((next & CONTINUATION_BIT) != 0);
      millis += delta;
      boolean isConfirmed =
          (confirmedBits[rowIndex / BITS_PER_BYTE] & (1 << (rowIndex % BITS_PER_BYTE))) != 0;
      if (isConfirmed) {
        long eventId = dictionary[Byte.toUnsignedInt(showCodes[rowIndex])];
        answer.merge(new MinuteWindows.WindowKey(eventId, minuteOf(millis)), 1L, Long::sum);
      }
    }
    return answer;
  }

  private static Instant minuteOf(long epochMillis) {
    long minuteMillis = MINUTE.toMillis();
    return Instant.ofEpochMilli(Math.floorDiv(epochMillis, minuteMillis) * minuteMillis);
  }
}
