# 📑 Hedge Audit Tool

[📄 **Live Demo**](https://fx-audit-toolgit.streamlit.app/)  

A Streamlit‑based application for **auditing hedge decisions against actual market outcomes**.  
Upload a CSV of predictions and hedge/no‑hedge calls, and the app will evaluate accuracy, error distribution, and overall effectiveness — then generate a professional, branded PDF report.

---

## 🚀 Features

- **CSV Upload & Sample Data**  
  Upload your own hedge history or use the built‑in sample dataset.

- **Automatic Date Handling**  
  Flexible parsing of messy date formats, with invalid rows gracefully excluded and reported.

- **Key Metrics Dashboard**  
  - Prediction Accuracy  
  - RMSE (Root Mean Square Error)  
  - Recall %  
  - Coverage & Missing Values  
  - Value‑Weighted Accuracy (if notionals provided)

- **Visual Analysis**  
  - Predicted vs. Live Rates chart  
  - Rolling 7‑day accuracy  
  - Error distribution histogram  
  - Top 5 largest prediction errors

- **Professional Reporting**  
  - Branded PDF export with executive summary and charts  
  - Downloadable audited CSV for further analysis

- **Robust Error Handling**  
  Clear warnings if data is missing, malformed, or partially excluded.

---

## 📸 Screenshots



- **Landing Page & Upload**  
  ![Upload Screen](docs/Screenshot.png)

- **Key Metrics Dashboard**  
  ![Metrics Dashboard](docs/screenshot_metrics.png)

- **Visual Analysis**  
  ![Charts](docs/screenshot_charts.png)

- **PDF Export**  
  ![PDF Export](docs/screenshot_pdf.png)

---

## 🌐 Live Demo

👉 Try the app instantly here: [fx-audit-toolgit.streamlit.app](https://fx-audit-toolgit.streamlit.app/)  
No installation required — just upload your CSV or use the sample dataset to see the audit in action.

---

## 📂 Example Input CSV

```csv
Timestamp,Predicted_Rate,Live_Rate,Decision,Pair
2023-01-01,0.6200,0.6100,Hedge,NZD/USD
2023-01-02,0.6050,0.6150,No Hedge,NZD/USD
```


##
Timestamp: ISO format preferred (YYYY-MM-DD), but flexible parsing supported

Predicted_Rate: Model’s forecasted rate

Live_Rate: Actual observed market rate

Decision: Hedge or No Hedge

Pair: Currency pair (e.g. NZD/USD)

##🛠️ Tech Stack
Python (pandas, numpy, matplotlib)

Streamlit for interactive UI

FPDF for PDF report generation

Docker & Streamlit Cloud ready for deployment

##📊 Use Cases
Corporate Treasury: Test hedge effectiveness and document compliance (IFRS 9).

Trading & Risk: Back‑test predictive models and stress‑test strategies.

Education: Teach hedging, forecasting, and error metrics in a hands‑on way.

General Decision Auditing: Adaptable to any binary decision based on forecasts (energy, supply chain, etc.).

##📦 Roadmap
Multi‑pair summary tables

Expanded provider fallback logic

Interactive parameter tuning (thresholds, rolling windows)

Cloud‑ready demo with one‑click sample run

##Author

built by Zane
