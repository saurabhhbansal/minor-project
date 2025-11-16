# backend/app/models.py

from sqlalchemy import Column, Integer, String, BigInteger, DateTime, UniqueConstraint, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Expenditure(Base):
    __tablename__ = "expenditures"
    id = Column(BigInteger, primary_key=True, index=True)
    institution_name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    year = Column(String, nullable=False)
    amount = Column(BigInteger)
    source_pdf = Column(String)
    pdf_sha256 = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    college_type = Column(String,nullable=False)
    __table_args__ = (
        UniqueConstraint('institution_name', 'category', 'year', 'pdf_sha256', name='unique_expenditure_record'),
    )
