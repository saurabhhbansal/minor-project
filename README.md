# **NIRF Budget Analysis System**

A full-stack web application that automates **NIRF PDF extraction**, **data cleaning**, **CSV generation**, **Supabase cloud storage**, and **machine-learning-based capital expenditure prediction** for Indian educational institutions.

---

## 🚀 **Tech Stack**
- **Frontend:** React (Vite)  
- **Backend:** FastAPI  
- **Database:** PostgreSQL  
- **Cloud Storage:** Supabase Storage  
- **ML Model:** Random Forest  
- **Custom Extraction:** Supports NIRF **2023 / 2024 / 2025** formats  

---

## 📦 **Features**

### ✔ **PDF Upload**
Upload NIRF PDFs for any institute. The backend automatically:

- Extracts tables & financial entries  
- Converts extracted data into a clean CSV  
- Stores both **PDF + CSV in Supabase Storage**  
- Saves metadata to PostgreSQL  
- Detects **UG / PG / Overall** PDF type  
- Computes **SHA-256 hash** to prevent duplicates  

---

### ✔ **File Viewer**
The frontend page `/view-data` displays:

- List of all stored PDFs  
- List of all generated CSVs  
- File size + download links  
- Auto-fetch via **FastAPI `/api/storage/list_all`**  

---

### ✔ **Machine Learning Prediction**
Predicts **Total Capital Expenditure** using features:

- Library  
- New Equipment for Laboratories  
- Engineering Workshop  
- Studio  
- Other Expenditure  

**Endpoints:**
- `POST /api/capital_predict`  
- `GET /api/feature_imp`  

---

### ✔ **Supabase Integration**
All uploaded files are stored securely in Supabase buckets:

- `pdfs/`  
- `csvs/`  

---

### ✔ **Full FastAPI REST Backend**
Backend includes the following endpoints:

- `/api/upload/pdf`  
- `/api/storage/list_all`  
- `/api/capital_predict`  
- `/api/feature_imp`  

---

### ✔ **Tableau Visualisations**
Integrated Tableau dashboards for interactive analysis.

---
