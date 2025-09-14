# Data Dictionary for Brazil Operations

This report profiles all raw CSV files to help build a unified data model.

## File: `03 LP Trades - LP (1).csv`

- **Status:** <font color='green'>Success</font>
- **Detected Separator:** `,`
- **Rows:** 122
- **Columns:** 38
- **Column Names:**
  - `Unnamed: 0`
  - `Unnamed: 1`
  - `Unnamed: 2`
  - `Unnamed: 3`
  - `Unnamed: 4`
  - `Unnamed: 5`
  - `Unnamed: 6`
  - `Unnamed: 7`
  - `Unnamed: 8`
  - `Unnamed: 9`
  - `Unnamed: 10`
  - `Unnamed: 11`
  - `Unnamed: 12`
  - `Unnamed: 13`
  - `Unnamed: 14`
  - `Unnamed: 15`
  - `Unnamed: 16`
  - `Unnamed: 17`
  - `Unnamed: 18`
  - `Unnamed: 19`
  - `Unnamed: 20`
  - `Unnamed: 21`
  - `Unnamed: 22`
  - `Unnamed: 23`
  - `Unnamed: 24`
  - `Unnamed: 25`
  - `Unnamed: 26`
  - `Unnamed: 27`
  - `Unnamed: 28`
  - `Unnamed: 29`
  - `Unnamed: 30`
  - `Unnamed: 31`
  - `Unnamed: 32`
  - `Unnamed: 33`
  - `Unnamed: 34`
  - `Unnamed: 35`
  - `Unnamed: 36`
  - `Unnamed: 37`

- **Data Sample:**
| Unnamed: 0   | Unnamed: 1   | Unnamed: 2      | Unnamed: 3   | Unnamed: 4   | Unnamed: 5   | Unnamed: 6   | Unnamed: 7   | Unnamed: 8         | Unnamed: 9       | Unnamed: 10   | Unnamed: 11   | Unnamed: 12    | Unnamed: 13    | Unnamed: 14   | Unnamed: 15   | Unnamed: 16   | Unnamed: 17   | Unnamed: 18   | Unnamed: 19     | Unnamed: 20     | Unnamed: 21          | Unnamed: 22       | Unnamed: 23   |   Unnamed: 24 |   Unnamed: 25 |   Unnamed: 26 |   Unnamed: 27 |   Unnamed: 28 |   Unnamed: 29 |   Unnamed: 30 |   Unnamed: 31 |   Unnamed: 32 |   Unnamed: 33 |   Unnamed: 34 |   Unnamed: 35 |   Unnamed: 36 |   Unnamed: 37 |
|:-------------|:-------------|:----------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------------|:-----------------|:--------------|:--------------|:---------------|:---------------|:--------------|:--------------|:--------------|:--------------|:--------------|:----------------|:----------------|:---------------------|:------------------|:--------------|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|
| Data de Hoje | 31/8/25 0:00 | nan             | nan          | nan          | nan          | nan          | nan          | nan                | nan              | nan           | nan           | nan            | nan            | nan           | nan           | nan           | nan           | nan           | nan             | nan             | nan                  | nan               | nan           |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |
| Trade ID     | Data         | Moeda/Crypto    | LP           | Volume       | nan          | Tx           | Volume BRL   | nan                | nan              | IOF           | Prazo         | Num dia        | Data Pagamento | IOF2          | nan           | Total         | Destino       | Cliente       | Moeda Cripto    | Outgoing Origin | Incoming Destination | Blockchain Dossie | Observações   |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |
| Trade ID     | Date         | Currency/Crypto | LP           | Volume       | LP Fee       | Fee          | Volume BRL   | LP Trade Fee (bps) | LP Trade Fee ($) | IOF Tax       | Term          | Number of Days | Payment Date   | IOF2          | Total (USD)   | Total (BRL)   | Destination   | Client        | Crypto Currency | Outgoing Origin | Incoming Destination | Blockchain Record | Notes         |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |

## File: `03 Pricer - BR - Florido - Pricer AK Finance  (1).csv`

- **Status:** <font color='green'>Success</font>
- **Detected Separator:** `,`
- **Rows:** 998
- **Columns:** 30
- **Column Names:**
  - `supp`
  - `data`
  - `sprd brz`
  - `spot`
  - `braza`
  - `spread`
  - `binance`
  - `binance 10 bps`
  - `spread.1`
  - `total spread`
  - `client price`
  - `ask price`
  - `vol BRL (given)`
  - `vol USDT (taken)`
  - `USDT Binance`
  - `fee binance`
  - `vol USDT (given)`
  - `vol BRL (taken)`
  - `Maturity`
  - `BRL supplier cost`
  - `BRL Supplier Braza`
  - `Total BRL buy order binance`
  - `BRL Supplier Binance`
  - `profit`
  - `base currency`
  - `quote currency`
  - `msg`
  - `msg pt-br`
  - `spread final`
  - `spread braza`

- **Data Sample:**
| supp   | data       |   sprd brz |   spot |   braza |   spread |   binance |   binance 10 bps |   spread.1 |   total spread |   client price |   ask price | vol BRL (given)   | vol USDT (taken)   |   USDT Binance |   fee binance |   vol USDT (given) | vol BRL (taken)   |   Maturity |   BRL supplier cost | BRL Supplier Braza   |   Total BRL buy order binance |   BRL Supplier Binance | profit     | base currency   | quote currency   | msg                                                                              | msg pt-br                                                       |   spread final |   spread braza |
|:-------|:-----------|-----------:|-------:|--------:|---------:|----------:|-----------------:|-----------:|---------------:|---------------:|------------:|:------------------|:-------------------|---------------:|--------------:|-------------------:|:------------------|-----------:|--------------------:|:---------------------|------------------------------:|-----------------------:|:-----------|:----------------|:-----------------|:---------------------------------------------------------------------------------|:----------------------------------------------------------------|---------------:|---------------:|
| braza  | 23/06/2025 |         15 |    nan |  5.5624 |      nan |       nan |              nan |         15 |            nan |         5.5707 |      5.5707 | R$500,000.00      | $89,755.33         |            nan |           nan |                nan | R$0.00            |          0 |              5.5669 | R$499,658.9500       |                           nan |                    nan | R$341.05   | USDT            | BRL              | Date: 31/08/2025                                                                 | Data: 31/08/2025                                                |           6.83 |          14.92 |
|        |            |            |        |         |          |           |                  |            |                |                |             |                   |                    |                |               |                    |                   |            |                     |                      |                               |                        |            |                 |                  |                                                                                  |                                                                 |                |                |
|        |            |            |        |         |          |           |                  |            |                |                |             |                   |                    |                |               |                    |                   |            |                     |                      |                               |                        |            |                 |                  | Rate is 5.5707 USDTBRL. You bought a total of 89,755.33 USDT for 500,000.00 BRL. | 5.5707 USDTBRL. Você comprou 89,755.33 USDT por 500,000.00 BRL. |                |                |
| braza  | 23/06/2025 |         15 |    nan |  5.5543 |      nan |       nan |              nan |         15 |            nan |         5.5626 |      5.5626 | R$350,000.00      | $62,920.22         |            nan |           nan |                nan | R$0.00            |          0 |              5.5569 | R$349,641.3700       |                           nan |                    nan | R$358.63   | USDT            | BRL              | Date: 31/08/2025                                                                 | Data: 31/08/2025                                                |          10.26 |          14.94 |
|        |            |            |        |         |          |           |                  |            |                |                |             |                   |                    |                |               |                    |                   |            |                     |                      |                               |                        |            |                 |                  |                                                                                  |                                                                 |                |                |
|        |            |            |        |         |          |           |                  |            |                |                |             |                   |                    |                |               |                    |                   |            |                     |                      |                               |                        |            |                 |                  | Rate is 5.5626 USDTBRL. You bought a total of 62,920.22 USDT for 350,000.00 BRL. | 5.5626 USDTBRL. Você comprou 62,920.22 USDT por 350,000.00 BRL. |                |                |
| braza  | 23/06/2025 |         17 |    nan |  5.5422 |      nan |       nan |              nan |         17 |            nan |         5.5516 |      5.5516 | R$450,000.00      | $81,057.71         |            nan |           nan |                nan | R$0.00            |          0 |              5.5392 | R$448,994.8700       |                           nan |                    nan | R$1,005.13 | USDT            | BRL              | Date: 31/08/2025                                                                 | Data: 31/08/2025                                                |          22.39 |          16.96 |
|        |            |            |        |         |          |           |                  |            |                |                |             |                   |                    |                |               |                    |                   |            |                     |                      |                               |                        |            |                 |                  |                                                                                  |                                                                 |                |                |
|        |            |            |        |         |          |           |                  |            |                |                |             |                   |                    |                |               |                    |                   |            |                     |                      |                               |                        |            |                 |                  | Rate is 5.5516 USDTBRL. You bought a total of 81,057.71 USDT for 450,000.00 BRL. | 5.5516 USDTBRL. Você comprou 81,057.71 USDT por 450,000.00 BRL. |                |                |

## File: `03 Pricer - BR - Pricer (1).csv`

- **Status:** <font color='green'>Success</font>
- **Detected Separator:** `,`
- **Rows:** 100
- **Columns:** 6
- **Column Names:**
  - `Unnamed: 0`
  - `Unnamed: 1`
  - `Unnamed: 2`
  - `Unnamed: 3`
  - `Unnamed: 4`
  - `Unnamed: 5`

- **Data Sample:**
| Unnamed: 0          | Unnamed: 1   | Unnamed: 2   | Unnamed: 3              | Unnamed: 4   |   Unnamed: 5 |
|:--------------------|:-------------|:-------------|:------------------------|:-------------|-------------:|
| DADOS DE NEGOCIAÇÃO | 5:46:11 AM   | nan          | Sunday, August 31, 2025 | nan          |     nan      |
| nan                 | 5.4130       | nan          | 541408.26               | Fator Union  |       1.004  |
| nan                 | R$ 5.4141    | 100.02%      | nan                     | IOF          |       1.0038 |

## File: `03 Sell USDT Trades - LP (1).csv`

- **Status:** <font color='green'>Success</font>
- **Detected Separator:** `,`
- **Rows:** 41
- **Columns:** 37
- **Column Names:**
  - `Unnamed: 0`
  - `Unnamed: 1`
  - `Unnamed: 2`
  - `Unnamed: 3`
  - `Unnamed: 4`
  - `Unnamed: 5`
  - `Unnamed: 6`
  - `Unnamed: 7`
  - `Unnamed: 8`
  - `Unnamed: 9`
  - `Unnamed: 10`
  - `Unnamed: 11`
  - `Unnamed: 12`
  - `Unnamed: 13`
  - `Unnamed: 14`
  - `Unnamed: 15`
  - `Unnamed: 16`
  - `Unnamed: 17`
  - `Unnamed: 18`
  - `Unnamed: 19`
  - `Unnamed: 20`
  - `Unnamed: 21`
  - `Unnamed: 22`
  - `Unnamed: 23`
  - `Unnamed: 24`
  - `Unnamed: 25`
  - `Unnamed: 26`
  - `Unnamed: 27`
  - `Unnamed: 28`
  - `Unnamed: 29`
  - `Unnamed: 30`
  - `Unnamed: 31`
  - `Unnamed: 32`
  - `Unnamed: 33`
  - `Unnamed: 34`
  - `Unnamed: 35`
  - `Unnamed: 36`

- **Data Sample:**
| Unnamed: 0   | Unnamed: 1   | Unnamed: 2      | Unnamed: 3   | Unnamed: 4   | Unnamed: 5   | Unnamed: 6   | Unnamed: 7         | Unnamed: 8        | Unnamed: 9   | Unnamed: 10   | Unnamed: 11    | Unnamed: 12    | Unnamed: 13   | Unnamed: 14   | Unnamed: 15   | Unnamed: 16   | Unnamed: 17   | Unnamed: 18     | Unnamed: 19     | Unnamed: 20          | Unnamed: 21       | Unnamed: 22   |   Unnamed: 23 |   Unnamed: 24 |   Unnamed: 25 |   Unnamed: 26 |   Unnamed: 27 |   Unnamed: 28 |   Unnamed: 29 |   Unnamed: 30 |   Unnamed: 31 |   Unnamed: 32 |   Unnamed: 33 |   Unnamed: 34 |   Unnamed: 35 |   Unnamed: 36 |
|:-------------|:-------------|:----------------|:-------------|:-------------|:-------------|:-------------|:-------------------|:------------------|:-------------|:--------------|:---------------|:---------------|:--------------|:--------------|:--------------|:--------------|:--------------|:----------------|:----------------|:---------------------|:------------------|:--------------|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|
| Data de Hoje | 31/8/25 0:00 | nan             | nan          | nan          | nan          | nan          | nan                | nan               | nan          | nan           | nan            | nan            | nan           | nan           | nan           | nan           | nan           | nan             | nan             | nan                  | nan               | nan           |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |
| Trade ID     | Data         | Moeda/Crypto    | LP           | Volume       | Tx           | Volume BRL   | nan                | nan               | IOF          | Prazo         | Num dia        | Data Pagamento | IOF2          | Total (BRL)   | Total (USD)   | Destino       | Cliente       | Moeda Cripto    | Outgoing Origin | Incoming Destination | Blockchain Dossie | Observações   |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |
| Trade ID     | Date         | Currency/Crypto | LP           | Volume       | Fee          | Volume BRL   | LP Trade Fee (bps) | LP Trade Fee (R$) | IOF Tax      | Term          | Number of Days | Payment Date   | IOF2          | Total (BRL)   | Total (USD)   | Destination   | Client        | Crypto Currency | Outgoing Origin | Incoming Destination | Blockchain Record | Notes         |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |           nan |

## File: `03 Trade History BI (Auto Updates) - Coins BR - Trade History - Coins.xyz Brazil (2).csv`

- **Status:** <font color='green'>Success</font>
- **Detected Separator:** `,`
- **Rows:** 722
- **Columns:** 42
- **Column Names:**
  - `Trade ID`
  - `Trade ID Supplier`
  - `FX Contract ID`
  - `Date`
  - `Operation`
  - `Client`
  - `Supplier`
  - `Coin`
  - `Sell Quantity`
  - `Outgoing Maturity`
  - `Incoming Maturity`
  - `Client Price`
  - `Sell Liq. Coin`
  - `Suplier Fee`
  - `Quotation Buy`
  - `Add. Cost Coin`
  - `USDT Conversion`
  - `Add. Value`
  - `Quotation Buy (Brl)`
  - `Amount Receivable`
  - `Amount Receivable (Approx USD)`
  - `Buy Quantity`
  - `IOF`
  - `Cost`
  - `Supplier/FX`
  - `Supplier/FX (Approx USD)`
  - `Supplier (IN1888)`
  - `Profit R$`
  - `% of Profit`
  - `Net Profit`
  - `Referral Partner`
  - `Referral Intermediary`
  - `Comission %`
  - `Comission Volume (Coin)`
  - `Partner Comission Volume ~ (BRL)`
  - `Referral Intermediary Comission Volume ~ (BRL)`
  - `Liq. Profit (Profit R$ - Comission)`
  - `Liq. Profit [Profit R$ - Comission] (Approx USD)`
  - `Net Profit - Comission (BRL)`
  - `Net Profit - Comission (USD)`
  - `% Final Profit`
  - `Outgoing Origin`

- **Data Sample:**
|   Trade ID |   Trade ID Supplier |   FX Contract ID | Date       | Operation   | Client                                                              | Supplier                                      | Coin   | Sell Quantity   |   Outgoing Maturity |   Incoming Maturity | Client Price   | Sell Liq. Coin   |   Suplier Fee | Quotation Buy   |   Add. Cost Coin |   USDT Conversion |   Add. Value | Quotation Buy (Brl)   |   Amount Receivable | Amount Receivable (Approx USD)   | Buy Quantity   |   IOF | Cost   | Supplier/FX   | Supplier/FX (Approx USD)   | Supplier (IN1888)   | Profit R$   | % of Profit    | Net Profit   |   Referral Partner |   Referral Intermediary | Comission %   | Comission Volume (Coin)   | Partner Comission Volume ~ (BRL)   |   Referral Intermediary Comission Volume ~ (BRL) | Liq. Profit (Profit R$ - Comission)   | Liq. Profit [Profit R$ - Comission] (Approx USD)   | Net Profit - Comission (BRL)   | Net Profit - Comission (USD)   | % Final Profit   |  Outgoing Origin                                          |
|-----------:|--------------------:|-----------------:|:-----------|:------------|:--------------------------------------------------------------------|:----------------------------------------------|:-------|:----------------|--------------------:|--------------------:|:---------------|:-----------------|--------------:|:----------------|-----------------:|------------------:|-------------:|:----------------------|--------------------:|:---------------------------------|:---------------|------:|:-------|:--------------|:---------------------------|:--------------------|:------------|:---------------|:-------------|-------------------:|------------------------:|:--------------|:--------------------------|:-----------------------------------|-------------------------------------------------:|:--------------------------------------|:---------------------------------------------------|:-------------------------------|:-------------------------------|:-----------------|:----------------------------------------------------------|
|    1000000 |                 nan |              nan | 05/04/2024 | otc         | MB INVESTIMENTOS INSTITUICAO DE PAGAMENTOS S.A (28.269.650/0001-56) | Capitual Pagamentos Ltda (50.815.803/0001-72) | USDT   | 59090,01        |                 nan |                   0 | 5,077000325    | BRL              |           nan | 5,075           |                1 |                 0 |          nan | 5,075                 |              300000 | 59090,01                         | 59090,01       |     1 | 5,075  | 299881,8008   | 59066,72868                | 299881,8008         | 118,19925   | 0,0003939975   | 105,3391716  |                nan |                     nan | 0             | 0                         | 0                                  |                                                0 | 118,19925                             | 23,29049261                                        | 109,1215476                    | 21,50178278                    | 0,0003939975     | trust-wallet (TRC20 - TXxjEJLLSfN9posU4HmH3cBREvo9oS7sMt) |
|    1000001 |                 nan |              nan | 05/04/2024 | otc         | MB INVESTIMENTOS INSTITUICAO DE PAGAMENTOS S.A (28.269.650/0001-56) | Capitual Pagamentos Ltda (50.815.803/0001-72) | USDT   | 39154,27        |                 nan |                   0 | 5,107999715    | BRL              |           nan | 5,0971          |                1 |                 0 |          nan | 5,0971                |              200000 | 39154,27                         | 39154,27       |     1 | 5,0971 | 199573,2296   | 39070,72059                | 199573,2296         | 426,770383  | 0,002133851915 | 380,3377653  |                nan |                     nan | 0,001         | 39,15427                  | 199,5732296                        |                                                0 | 227,1971534                           | 44,57380734                                        | 209,748412                     | 41,15053893                    | 0,001135985767   | trust-wallet (TRC20 - TXxjEJLLSfN9posU4HmH3cBREvo9oS7sMt) |
|    1000002 |                 nan |              nan | 08/04/2024 | otc         | MB INVESTIMENTOS INSTITUICAO DE PAGAMENTOS S.A (28.269.650/0001-56) | Capitual Pagamentos Ltda (50.815.803/0001-72) | USDT   | 58731,4         |                 nan |                   0 | 5,10800015     | BRL              |           nan | 5,0933          |                1 |                 0 |          nan | 5,0933                |              300000 | 58731,4                          | 58731,4        |     1 | 5,0933 | 299136,6396   | 58562,37879                | 299136,6396         | 863,36038   | 0,002877867933 | 769,4267707  |                nan |                     nan | 0,001         | 58,7314                   | 299,1366396                        |                                                0 | 564,2237404                           | 110,7776374                                        | 520,8913571                    | 102,2699148                    | 0,001880745801   | trust-wallet (TRC20 - TXxjEJLLSfN9posU4HmH3cBREvo9oS7sMt) |

## File: `03 TradeDesk (BRAZIL)  - PNL & Volume Report - Weekly Summary (2).csv`

- **Status:** <font color='green'>Success</font>
- **Detected Separator:** `,`
- **Rows:** 82
- **Columns:** 22
- **Column Names:**
  - `FX Summary`
  - `Unnamed: 1`
  - `Unnamed: 2`
  - `Unnamed: 3`
  - `Unnamed: 4`
  - `OTC Summary`
  - `Unnamed: 6`
  - `Unnamed: 7`
  - `Unnamed: 8`
  - `Unnamed: 9`
  - `Unnamed: 10`
  - `TradeDesk Summary`
  - `Unnamed: 12`
  - `Unnamed: 13`
  - `Unnamed: 14`
  - `Unnamed: 15`
  - `Unnamed: 16`
  - `Unnamed: 17`
  - `Unnamed: 18`
  - `USD/BRL ref`
  - `5.5`
  - `*Income stays in BRL, values in USD are approximations due to USD/BRL rate`

- **Data Sample:**
|   FX Summary | Unnamed: 1       | Unnamed: 2   |   Unnamed: 3 |   Unnamed: 4 | OTC Summary      | Unnamed: 6     |   Unnamed: 7 |   Unnamed: 8 |   Unnamed: 9 |   Unnamed: 10 | TradeDesk Summary   | Unnamed: 12    |   Unnamed: 13 |   Unnamed: 14 |   Unnamed: 15 |   Unnamed: 16 |   Unnamed: 17 |   Unnamed: 18 |   USD/BRL ref |   5.5 |   *Income stays in BRL, values in USD are approximations due to USD/BRL rate |
|-------------:|:-----------------|:-------------|-------------:|-------------:|:-----------------|:---------------|-------------:|-------------:|-------------:|--------------:|:--------------------|:---------------|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|--------------:|------:|-----------------------------------------------------------------------------:|
|         2024 | YTD INCOME (USD) | $0.00        |          nan |          nan | YTD INCOME (USD) | $18,763.80     |          nan |          nan |          nan |           nan | YTD INCOME (USD)    | $18,763.80     |           nan |           nan |           nan |           nan |           nan |           nan |           nan |   nan |                                                                          nan |
|          nan | YTD $ VOLUME     | $0.00        |          nan |          nan | YTD $ VOLUME     | $15,973,048.23 |          nan |          nan |          nan |           nan | YTD $ VOLUME        | $15,973,048.23 |           nan |           nan |           nan |           nan |           nan |           nan |           nan |   nan |                                                                          nan |
|          nan | BPS              | 0.0          |          nan |          nan | BPS              | 11.7           |          nan |          nan |          nan |           nan | BPS                 | 11.7           |           nan |           nan |           nan |           nan |           nan |           nan |           nan |   nan |                                                                          nan |

## File: `Analysis-Analysis 2.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `Analysis-Analysis 3.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `Analysis-Analysis 4.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `Analysis-Analysis 5.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `Analysis-Analysis.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `Analysis-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `Conciliação-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `Credit Card History-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `History-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `credit card - analysis-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `credit card - analysis-credit card - analysis 2.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `credit card - analysis-credit card - analysis.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `dropdown-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `embededLinks-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `tax analysis-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

## File: `taxes-Table 1.csv`

- **Status:** <font color='red'>Failed</font>
- **Error:** The 'low_memory' option is not supported with the 'python' engine

