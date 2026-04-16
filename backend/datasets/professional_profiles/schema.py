from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


ProfileStatus = Literal["active", "inactive", "draft"]


class PersonaModel(BaseModel):
    archetype: str = ""
    summary: str = ""
    work_style: list[str] = Field(default_factory=list)
    behavioral_traits: list[str] = Field(default_factory=list)
    decision_style: list[str] = Field(default_factory=list)
    ideal_signals: list[str] = Field(default_factory=list)
    risk_signals: list[str] = Field(default_factory=list)


class ProfessionalProfile(BaseModel):
    role_id: str
    title: str
    family: str = ""
    hub: str = ""
    seniority: str = ""
    work_model: str = ""
    location: str = ""
    summary: str = ""

    responsibilities: list[str] = Field(default_factory=list)
    required_skills: list[str] = Field(default_factory=list)
    preferred_skills: list[str] = Field(default_factory=list)
    tools: list[str] = Field(default_factory=list)
    experience_requirements: list[str] = Field(default_factory=list)
    education_requirements: list[str] = Field(default_factory=list)

    keywords: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)

    persona: PersonaModel = Field(default_factory=PersonaModel)

    source_docs: list[str] = Field(default_factory=list)
    version: int = 1
    status: ProfileStatus = "active"
    updated_at: str = ""

    def search_text(self) -> str:
        parts = [
            self.role_id,
            self.title,
            self.family,
            self.hub,
            self.seniority,
            self.work_model,
            self.location,
            self.summary,
            *self.responsibilities,
            *self.required_skills,
            *self.preferred_skills,
            *self.tools,
            *self.experience_requirements,
            *self.education_requirements,
            *self.keywords,
            *self.aliases,
            self.persona.archetype,
            self.persona.summary,
            *self.persona.work_style,
            *self.persona.behavioral_traits,
            *self.persona.decision_style,
            *self.persona.ideal_signals,
            *self.persona.risk_signals,
        ]
        return " | ".join([str(x).strip() for x in parts if str(x).strip()])


class ProfessionalProfilesCatalog(BaseModel):
    profiles: list[ProfessionalProfile] = Field(default_factory=list)