NIRF Budget Analysis System

A full-stack web application that automates NIRF PDF extraction, data cleaning, CSV generation, Supabase cloud storage, and machine-learning-based capital expenditure prediction for Indian educational institutions.

This project uses:

React (Vite) for the frontend

FastAPI for the backend

PostgreSQL for storing extracted financial data

Supabase Storage for storing uploaded PDFs & generated CSV files

ML model (Random Forest) for predicting total capital expenditure

Custom extraction pipeline for NIRF 2023/2024/2025 formats

📦 Features
✔ PDF Upload

Upload NIRF PDFs for any institute. The backend:

Extracts tables & financial entries

Converts the extracted data to a clean CSV

Stores PDF + CSV in Supabase Storage

Saves metadata to PostgreSQL

Detects UG/PG/Overall PDF type

Computes SHA-256 hash to avoid duplicate uploads

✔ File Viewer

Frontend page /view-data displays:

List of all PDFs stored

List of generated CSVs

File size + download links

Auto-fetch from FastAPI /api/storage/list_all

✔ Machine Learning Prediction

Predicts Total Capital Expenditure using:

Library

New Equipment for Laboratories

Engineering Workshop

Studio

Other expenditure

Endpoints:

POST /api/capital_predict
GET /api/feature_imp

✔ Supabase Integration

All uploaded files (PDF/CSV) are pushed to Supabase buckets:

pdfs/

csvs/

✔ Full FastAPI REST Backend

Includes endpoints:

/api/upload/pdf
/api/storage/list_all
/api/capital_predict
/api/feature_imp

✔ Tableau visualisations
