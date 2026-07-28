package models

import "gorm.io/gorm"

type User struct {
	gorm.Model
	Email string `gorm:"size:255;not null;uniqueIndex"`
	Pets  []Pet  `gorm:"constraint:OnDelete:CASCADE"`
}

type Pet struct {
	gorm.Model
	Name   string `gorm:"size:100;not null"`
	UserID uint   `gorm:"not null;index"`
}
