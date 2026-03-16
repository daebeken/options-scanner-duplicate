# Sources:
# https://www.talos.com/insights/introducing-implied-volatility-surface-metrics
# https://docs.coinmetrics.io/market-data/market-data-overview/volatility/implied-volatility-constant-maturity-at-the-money
# https://moontowermeta.com/volatility-term-structure-from-multiple-angles-part-1/

library(data.table)
library(finutils)
library(ggplot2)
library(PerformanceAnalytics)
library(patchwork)
library(AzureStor)
use("dplyr", "ntile")

# utils
number_of_duplicated = function(dt) {
  nrow(dt[duplicated(dt, by = c("ticker", "trade_date"))]) / nrow(dt) * 100
}

# Import atm constant maturity options data
options = fread("data/atm_options.csv")







# Calculate ATM Straddle Implied Volatility
options[, atm_iv := (cMidIv + pMidIv) / 2]

# Calculate days to expiration
options[, days_to_expiry := yte * 365]

# Keep data ordered for faster within-group lookups
setkey(options, ticker, trade_date, days_to_expiry)

# My Way to calculate Constant maturity IV term structure
dte   = c(21, 30, 60, 90, 120, 180, 270, 365)
cols = paste0("IV", dte, "D")
# test = options[1:1000]
options[, (cols) := {
  # 1/distance weighting between nearest maturities equals linear interpolation.
  x = days_to_expiry
  y = atm_iv
  ok = is.finite(x) & is.finite(y)
  if (!any(ok)) {
    as.list(rep(NA_real_, length(dte)))
  } else {
    xok = x[ok]
    yok = y[ok]
    if (length(unique(xok)) < 2L) {
      as.list(rep(yok[1], length(dte)))
    } else {
      as.list(approx(x = xok, y = yok, xout = dte, rule = 2, ties = mean)$y)
    }
  }
}, by = .(ticker, trade_date)]

# Inspect spreads
spreads = options[, .(c_spread = cAskPx - cBidPx, midprice = (cAskPx + cBidPx)/2)] |>
  _[, .(c_spread_pct = c_spread / midprice)]
summary(spreads)
spreads = options[, .(c_spread = cAskPx - cBidPx, midprice = (cAskPx + cBidPx)/2), by = ticker] |>
  _[, c_spread_pct := c_spread / midprice]
spreads = spreads[, .(
  mean_   = mean(c_spread_pct),
  median_ = median(c_spread_pct)
), by = ticker]
spreads[, hist(mean_)]
spreads[, hist(median_)]


# PRICES ------------------------------------------------------------------
# Import data
tickers = options[, tolower(unique(ticker))]
tickers = c(tickers, paste0(tickers, ".1"))
tickers = c(tickers, paste0(tickers, ".2"))
prices = qc_daily_parquet(
  file_path = file.path("C:/Users/Mislav/qc_snp/data", "all_stocks_daily"),
  symbols = tickers,
  etfs = FALSE,
  # etf_cons = "spy",
  min_obs = 252,
  price_threshold = 1e-008,
  duplicates = "fast",
  add_dv_rank = FALSE 
)

# Be aware the key is set
key(prices)

# Fix symbol column
prices[, opt_symbol := gsub("\\..*", "", toupper(symbol))]
max_dates = prices[, .(max_date = max(date)), by = .(opt_symbol, symbol)]
one_symbol_per_ajd = max_dates[ , .SD[which.max(max_date)], by = opt_symbol]
prices = prices[one_symbol_per_ajd, on = .(opt_symbol, symbol), nomatch = 0]
prices[, symbol := gsub("\\..*", "", toupper(symbol))]
prices[, opt_symbol := NULL]
ggplot(prices[symbol == "AAPL", .(symbol, date, close)], aes(x = date, y = close, color = symbol)) +
  geom_line()

# Calculate returns
setorder(prices, symbol, date)
prices[, returns := close / shift(close) - 1, by = symbol]
prices[, logret  := log(close) - log(shift(close)), by = symbol]

# Rolling realized volatility (annualized; uses RMS of daily log-returns)
# Map calendar days-to-expiry to ~trading-day windows (252 trading days / year, 365 calendar).
td = function(d) as.integer(round(d * 252 / 365))
prices[, RV21D  := sqrt(252 * frollmean(logret^2, n = 21,      na.rm = TRUE)), by = symbol] # ~1 trading month
prices[, RV30D  := sqrt(252 * frollmean(logret^2, n = td(30),  na.rm = TRUE)), by = symbol] # matches IV30D tenor
prices[, RV180D := sqrt(252 * frollmean(logret^2, n = td(180), na.rm = TRUE)), by = symbol] # matches IV180D tenor
prices[, RV252D := sqrt(252 * frollmean(logret^2, n = 252,     na.rm = TRUE)), by = symbol] # ~1 trading year

# Backwards-compatible name (was 252d realized vol)
prices[, RVLT := RV252D]

# Inspect realized volatiltiy
prices[, hist(RVLT)]
prices[, summary(RVLT)]

# Create rank by volume for every date
prices[, dollar_vol_rank := frankv(close_raw * volume, order = -1L), by = date]
prices[date == sample(prices[, unique(date)], 1)][order(dollar_vol_rank)][1:20]

# Merge realized volatility
options_dt = merge(
  options,
  prices[, .(symbol, date, RVLT, RV21D, RV30D, RV180D, RV252D, close, open, dollar_vol_rank)],
  by.x = c("ticker", "trade_date"),
  by.y = c("symbol", "date"),
  all.x = TRUE,
  all.y = FALSE
)

# Order
setorder(options_dt, ticker, trade_date)

# Ensure IVs are in decimal (0.20 not 20). If your inputs are already decimals, this is a no-op.
iv_cols = grep("^IV\\d+D$", names(options_dt), value = TRUE)
options_dt[, (iv_cols) := lapply(.SD, function(x) fifelse(is.finite(x) & x > 3, x / 100, x)), .SDcols = iv_cols]

# IV vs RV (match 30D tenor; compare in variance)
options_dt[, IVRV_VAR_30D := (IV30D^2 - RV30D^2)]
options_dt[, IVRV_VAR_30D_PCT := IVRV_VAR_30D / RV30D^2]

# Term-structure via forward variance between 30D and 180D (days as weights)
options_dt[, FWD_VAR_30_180 := (IV180D^2 * 180 - IV30D^2 * 30) / (180 - 30)]
options_dt[, FWD_VOL_30_180 := sqrt(pmax(FWD_VAR_30_180, 0))]
options_dt[, IV_FWD_SLOPE_30_180 := (FWD_VOL_30_180 - IV30D) / IV30D]

# Backwards-compatible names (keep your originals)
options_dt[, IVRV_SLOPE := (RVLT - IV30D) / RVLT]
options_dt[, IV_SLOPE   := (IV180D - IV30D) / IV180D]


# TRADING -----------------------------------------------------------------
# 1) Precompute straddle mid for every contract (keep on merged table so slope features are available)
options_dt[, straddle_mid := (cBidPx + cAskPx + pBidPx + pAskPx) / 2]

# 2) Key once for nearest-lookups on all expiries (no weekly-only filter)
setkey(options_dt, ticker, trade_date, days_to_expiry)

# 3) Helper: pick the expiry nearest to target_dte with a tolerance
pick_straddle = function(target_dte, tol = 5) {
  trg = unique(options_dt[, .(ticker, trade_date)])
  trg[, target_dte := target_dte]

  res = options_dt[
    trg,
    roll = "nearest",
    on = .(ticker, trade_date, days_to_expiry = target_dte)
  ]

  res = res[abs(days_to_expiry - target_dte) <= tol]
  setorder(res, ticker, trade_date)
  res[, .(ticker, trade_date, expirDate, days_to_expiry,
          straddle_mid, cBidPx, cAskPx, pBidPx, pAskPx,
          cMidIv, pMidIv, IV30D, IV180D, IVRV_SLOPE, IV_SLOPE, RVLT)]
}

# Example pulls (you can still trade weekly by sampling trade_date weekly downstream)
straddle_7d  = pick_straddle(7,  tol = 3)
straddle_30d = pick_straddle(30, tol = 14)

# Add bid/ask legs and spreads for quality control
add_spread_cols = function(dt) {
  dt[
    , `:=`(
      straddle_bid     = cBidPx + pBidPx,
      straddle_ask     = cAskPx + pAskPx,
      straddle_mid     = (cBidPx + pBidPx + cAskPx + pAskPx) / 2,
      spread_abs       = (cAskPx + pAskPx) - (cBidPx + pBidPx),
      spread_pct_mid   = ((cAskPx + pAskPx) - (cBidPx + pBidPx)) / ((cBidPx + pBidPx + cAskPx + pAskPx) / 2)
    )
  ]
}
straddle_7d  = add_spread_cols(straddle_7d)
straddle_30d = add_spread_cols(straddle_30d)

# Weekly slice with spread filter
weekly_slice = straddle_30d[
  as.POSIXlt(as.Date(trade_date))$wday == 3 # Wednesday (0=Sun, 3=Wed)
    # spread_pct_mid <= 0.20              # tighten/loosen as needed
]

# Ensure no dup per ticker/date (tolerance already applied)
stopifnot(anyDuplicated(weekly_slice, by = c("ticker", "trade_date")) == 0)

# Number of symbols through time
ggplot(weekly_slice[, .N, by = trade_date], aes(trade_date, N)) + geom_line()

# Compute trade P&L (example: hold to next week’s mark)
weekly_slice[, `:=`(
  next_mid = shift(straddle_mid, type = "lead"),
  next_bid = shift(straddle_bid, type = "lead"),
  next_ask = shift(straddle_ask, type = "lead")
), by = ticker]

# Returns across execution quality: mid→mid (best), bid/ask (worst), plus 5 evenly spaced fills
fill_grid = seq(0, 1, length.out = 7)  # 0 = both legs at mid, 1 = pay ask in / hit bid out
ret_cols = paste0("ret_fill_", sprintf("%02.0f", fill_grid * 100))
weekly_slice[, (ret_cols) := {
  lapply(fill_grid, function(f) {
    entry = straddle_mid + f * (straddle_ask - straddle_mid)
    exit  = next_mid      - f * (next_mid - next_bid)
    exit / entry - 1
  })
}]
short_ret_cols = paste0("short_ret_fill_", sprintf("%02.0f", fill_grid * 100))
weekly_slice[, (short_ret_cols) := {
  lapply(fill_grid, function(f) {
    entry = straddle_mid - f * (straddle_mid - straddle_bid)   # sell nearer bid as f↑
    exit  = next_mid      + f * (next_ask - next_mid)          # buy back nearer ask as f↑
    entry / exit - 1
  })
}]

# Remove NA returns
cols_long  = grep("^ret_fill_", colnames(weekly_slice), value = TRUE)
cols_short = grep("^short_ret_fill_", colnames(weekly_slice), value = TRUE)
weekly_slice = na.omit(weekly_slice, cols = c(cols_long, cols_short))

# Return summary
weekly_slice[, summary(.SD), .SDcols = patterns("ret_")]
weekly_slice[, hist(ret_fill_00)]
weekly_slice[ret_fill_00  %between% c(-0.7, 0.7), hist(ret_fill_00)]
weekly_slice[ret_fill_100 %between% c(-0.7, 0.7), hist(ret_fill_100)]

# Attach signal (e.g., IV slope decile) and test
weekly_slice[, q_iv := ntile(IV_SLOPE, 10), by = trade_date]
weekly_slice[, q_rv := ntile(RVLT, 10), by = trade_date]

g_best_1 = weekly_slice[, .(ret = mean(ret_fill_00, na.rm = TRUE)), by = q_iv][order(q_iv)] |>
  ggplot(aes(q_iv, ret)) +
  geom_bar(stat="identity")
g_best_2 = weekly_slice[, .(ret = mean(ret_fill_33, na.rm = TRUE)), by = q_iv][order(q_iv)] |>
  ggplot(aes(q_iv, ret)) +
  geom_bar(stat="identity")
g_best_3 = weekly_slice[, .(ret = mean(ret_fill_67, na.rm = TRUE)), by = q_iv][order(q_iv)] |>
  ggplot(aes(q_iv, ret)) +
  geom_bar(stat="identity")
g_best_4 = weekly_slice[, .(ret = mean(ret_fill_100, na.rm = TRUE)), by = q_iv][order(q_iv)] |>
  ggplot(aes(q_iv, ret)) +
  geom_bar(stat="identity")
(g_best_1 + g_best_2) / (g_best_3 + g_best_4)

g_best_1 = weekly_slice[, .(ret = median(ret_fill_00, na.rm = TRUE)), by = q_iv][order(q_iv)] |>
  ggplot(aes(q_iv, ret)) +
  geom_bar(stat="identity")
g_best_2 = weekly_slice[, .(ret = median(ret_fill_33, na.rm = TRUE)), by = q_iv][order(q_iv)] |>
  ggplot(aes(q_iv, ret)) +
  geom_bar(stat="identity")
g_best_3 = weekly_slice[, .(ret = median(ret_fill_67, na.rm = TRUE)), by = q_iv][order(q_iv)] |>
  ggplot(aes(q_iv, ret)) +
  geom_bar(stat="identity")
g_best_4 = weekly_slice[, .(ret = median(ret_fill_100, na.rm = TRUE)), by = q_iv][order(q_iv)] |>
  ggplot(aes(q_iv, ret)) +
  geom_bar(stat="identity")
(g_best_1 + g_best_2) / (g_best_3 + g_best_4)

g_best_1 = weekly_slice[, .(ret = mean(ret_fill_00, na.rm = TRUE)), by = q_rv][order(q_rv)] |>
  ggplot(aes(q_rv, ret)) +
  geom_bar(stat="identity")
g_best_2 = weekly_slice[, .(ret = mean(ret_fill_33, na.rm = TRUE)), by = q_rv][order(q_rv)] |>
  ggplot(aes(q_rv, ret)) +
  geom_bar(stat="identity")
g_best_3 = weekly_slice[, .(ret = mean(ret_fill_67, na.rm = TRUE)), by = q_rv][order(q_rv)] |>
  ggplot(aes(q_rv, ret)) +
  geom_bar(stat="identity")
g_best_4 = weekly_slice[, .(ret = mean(ret_fill_100, na.rm = TRUE)), by = q_rv][order(q_rv)] |>
  ggplot(aes(q_rv, ret)) +
  geom_bar(stat="identity")
(g_best_1 + g_best_2) / (g_best_3 + g_best_4)

# Simple backtest
long  = weekly_slice[q_iv == 10][, weights := 1 / .N, by = trade_date][order(trade_date)]
short = weekly_slice[q_iv == 1][, weights := 1 / .N, by = trade_date][order(trade_date)]
portfolio_long = long[, lapply(cols_long, function(x) sum(weights * get(x))), by = trade_date]
setnames(portfolio_long, c("trade_date", cols_long))
portfolio_short = short[, lapply(cols_short, function(x) sum(weights * get(x))), by = trade_date]
setnames(portfolio_short, c("trade_date", cols_short))
charts.PerformanceSummary(as.xts.data.table(portfolio_long))
charts.PerformanceSummary(as.xts.data.table(portfolio_short))
charts.PerformanceSummary(as.xts.data.table(portfolio_long[, .SD, .SDcols = c(1, 3)]))
charts.PerformanceSummary(as.xts.data.table(portfolio_long[, .SD, .SDcols = c(1, 4)]))
charts.PerformanceSummary(as.xts.data.table(portfolio_long[, .SD, .SDcols = c(1, 5)]))
portfolio_ls = merge(
  portfolio_long,
  portfolio_short,
  by = "trade_date",
  all = FALSE,
  sort = TRUE
)
ls_cols = paste0("ls_fill_", sprintf("%02.0f", fill_grid * 100))
long_mat  = as.matrix(portfolio_ls[, ..cols_long])
short_mat = as.matrix(portfolio_ls[, ..cols_short])
portfolio_ls[, (ls_cols) := as.data.table((long_mat + short_mat) / 2)]
charts.PerformanceSummary(as.xts.data.table(portfolio_ls[, .SD, .SDcols = c("trade_date", ls_cols)]))
charts.PerformanceSummary(as.xts.data.table(portfolio_ls[, .SD, .SDcols = c(1:5)]))

# Buy /sell n symbols
n = 20
# weekly_slice[, summary(IV_SLOPE)]
# weekly_slice[, mean(IV_SLOPE), by = q_iv][order(q_iv)]
long  = weekly_slice[q_iv == 10] |>
  _[order(-IV_SLOPE)]  |>
  _[, head(.SD, n), by = trade_date] |>
  _[, weights := 1 / .N, by = trade_date][order(trade_date)]
portfolio_long = long[, lapply(cols_long, function(x) sum(weights * get(x))), by = trade_date]
setnames(portfolio_long, c("trade_date", cols_long))
charts.PerformanceSummary(as.xts.data.table(portfolio_long))
charts.PerformanceSummary(as.xts.data.table(portfolio_long[, .SD, .SDcols = c(1, 6)]))

# Save to QC
qc_data = long[, .(date = trade_date, ticker)]
setorder(qc_data, date)
qc_data = qc_data[, .(symbol = paste0(ticker, collapse = "|")), by = date]
qc_data[, date := as.character(date)]
qc_data[, date := paste0(date, " 15:00:00")]
blob_key = Sys.getenv("BLOB-KEY-SNP")
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "qc-backtest")
delete_storage_file(cont, "tsiv.csv", confirm = FALSE)
storage_write_csv(qc_data, cont, "tsiv.csv")

# # Inspect differences between QC and local
# back_q[trade_date == as.Date("2023-01-01")]
# head(back_q[trade_date > as.Date("2023-01-01")], 40)
# back_q[trade_date > as.Date("2023-01-01")][ticker == "DVN"]