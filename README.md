# NYC Taxi Pricing & Demand Analysis 🚕

A comprehensive analysis of NYC taxi pricing strategies and demand patterns across four analytical modules.

## 📋 Project Overview

This project analyzes NYC Taxi data through four modules to explore pricing strategies, demand patterns, and revenue optimization opportunities.

**Team Members:**
- Module 1 (Demand Analysis): Harvey
- Module 2 (Price Sensitivity): Jeremy  
- Module 3 (Supply-Demand Imbalance): Royan
- Module 4 (Revenue Efficiency): Desmond Peng

## 🗂️ Project Structure

```
nyc-taxi-pricing-analysis/
├── data/
│   ├── raw/                    # Raw data (not in git)
│   ├── processed/              # Cleaned data
│   └── interim/                # Intermediate data
│
├── notebooks/
│   ├── 00_data_preparation.ipynb
│   ├── 01_demand_analysis_harvey.ipynb
│   ├── 02_price_sensitivity_jeremy.ipynb
│   ├── 03_supply_demand_imbalance.ipynb
│   ├── 04_revenue_efficiency_desmond.ipynb
│   └── 05_integrated_insights.ipynb
│
├── src/
│   ├── config.py                  # Global configuration
│   ├── data_loader.py             # Data loading & preprocessing
│   ├── utils.py                   # Shared utilities
│   ├── module1_demand/            # Demand analysis module
│   ├── module2_price/             # Price sensitivity module
│   ├── module3_imbalance/         # Supply-demand imbalance module
│   └── module4_revenue/           # Revenue efficiency module
│
├── results/
│   ├── figures/                   # Visualizations by module
│   ├── tables/                    # Output CSV files
│   └── presentation/              # Presentation materials
│
├── tests/
├── requirements.txt
└── run_analysis.py                # Main execution script
```

---

## 🎯 Four Analysis Modules

### Module 1: Demand Analysis - Harvey

**Objective:** Analyze trip volume patterns across regions and time periods

**Key Metrics:**
- Hourly/daily/weekly trip counts
- Peak and off-peak periods by region
- Weekday vs weekend patterns
- Demand volatility indicators

**Outputs:**
- `demand_heatmap.png` - Time × region heatmap
- `peak_hour_ranking.png` - Peak hours by zone
- `weekday_weekend_comparison.png` - Weekday/weekend comparison
- `demand_volatility.csv` - Volatility metrics

**Example Insight:**  
*"Manhattan demand surges Friday 6-9 PM; Brooklyn shows steady weekend afternoon demand with longer distances"*

---

### Module 2: Price Sensitivity Analysis - Jeremy

**Objective:** Compare price sensitivity across different regions

**Key Metrics:**
- Price quantile grouping (low/medium/high)
- Demand change rate during high-price periods
- Low-sensitivity region identification

**Outputs:**
- `price_band_demand.csv` - Demand by price band
- `sensitivity_ranking.png` - Sensitivity rankings
- `price_variation_chart.png` - Price variation distribution

**Example Insight:**  
*"JFK area shows only 3% demand drop in high-price band (low sensitivity); Queens residential areas drop 18% (high sensitivity)"*

---

### Module 3: Supply-Demand Imbalance Analysis

**Objective:** Identify high-demand, low-supply opportunities for price adjustments

**Key Metrics:**
- Load Ratio = trip_count / rolling_median(prior N hours)
- High load_ratio regions and time periods
- Demand surge scenarios with unadjusted pricing

**Outputs:**
- `imbalance_heatmap.png` - Load ratio distribution
- `surge_candidates.csv` - Potential price adjustment periods
- `top_imbalance_zones.csv` - High-demand, low-price zones

**Example Insight:**  
*"Midtown Friday 5-7 PM shows 1.4x normal demand but only slightly higher prices → opportunity for moderate price adjustment"*

---

### Module 4: Revenue & Efficiency Analysis - Desmond Peng

**Objective:** Analyze highest revenue potential and efficiency zones

**Key Metrics:**
- Revenue = trip_count × avg_price × avg_distance
- Borough revenue contribution comparison
- High-price low-volume vs low-price high-volume zones
- Trip efficiency analysis

**Outputs:**
- `revenue_heatmap.png` - Revenue distribution
- `price_revenue_scatter.png` - Price vs revenue relationship
- `efficiency_matrix.csv` - Efficiency matrix
- `zone_profit_ranking.csv` - Top 10 revenue potential zones

**Example Insight:**  
*"JFK area: high prices, low volume but top 5% revenue; Brooklyn off-peak: low prices, stable volume, high efficiency"*

---

## 🚀 Quick Start

### 1. Environment Setup

```bash
# Clone repository
git clone https://github.com/Jeremy891102/NYC-Taxi-Pricing-Demand-Analysis.git
cd nyc-taxi-pricing-analysis

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Data Preparation

```bash
# Run data preprocessing
python src/data_loader.py

# Or use notebook
jupyter notebook notebooks/00_data_preparation.ipynb
```

### 3. Run Analysis

```bash
# Run full analysis
python run_analysis.py

# Or run specific module
python run_analysis.py --module 1
```

---

## 📊 Code Management

### Git Workflow

```bash
# Create feature branches
git checkout -b module1-harvey
git checkout -b module2-jeremy
git checkout -b module4-desmond

# Merge to main
git checkout main
git merge module1-harvey
```

### Code Style

- **Variables:** lowercase_with_underscores (`trip_count`, `avg_price`)
- **Functions:** verb_start (`calculate_demand`, `plot_heatmap`)
- **Comments:** Add comments for key logic
- **Docstrings:** Include docstrings for important functions

---

## 📈 Deliverables

Each module should produce:

1. **Jupyter Notebook** (`.ipynb`) - Complete analysis with reproducible code
2. **Visualizations** (save to `results/figures/moduleX/`) - High-res PNG and interactive HTML
3. **Data Tables** (save to `results/tables/`) - CSV files with documentation
4. **Key Findings** (update `results/presentation/key_findings.md`) - 3-5 core insights with chart references

### Presentation Timeline

- **Week 1-2:** Individual module completion → Submit PR
- **Week 3:** Integrated analysis (`05_integrated_insights.ipynb`) → Cross-validate findings
- **Week 4:** Presentation preparation → Select charts → Create slides

---

## 🛠️ Tech Stack

```
pandas >= 2.0.0
numpy >= 1.24.0
matplotlib >= 3.7.0
seaborn >= 0.12.0
plotly >= 5.14.0
scipy >= 1.10.0
scikit-learn >= 1.2.0
geopandas >= 0.12.0
folium >= 0.14.0
jupyter >= 1.0.0
```

---

## 📝 Notes

- **Data Security:** Raw data not in git (add to `.gitignore`)
- **Version Control:** Regular commits, one feature per commit
- **Code Review:** At least one reviewer before PR
- **Team Sync:** Weekly team meetings to align progress

---

**Last Updated:** 2025-01-12
