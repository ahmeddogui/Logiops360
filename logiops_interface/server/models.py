from __future__ import annotations

import uuid
from enum import Enum

from sqlalchemy import Column, DateTime, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base


class TypeProfil(str, Enum):
    commande = "commande"
    stockage = "stockage"
    transport = "transport"
    superviseur = "superviseur"


Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    nom = Column(String, nullable=False)
    email = Column(String, nullable=False)
    mot_de_passe_hash = Column(String, nullable=False)
    type_profil = Column(String, nullable=False)
    date_creation = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("email", "type_profil", name="uq_user_email_profile"),
    )

    def __repr__(self) -> str:
        return f"<User {self.email} ({self.type_profil})>" 
