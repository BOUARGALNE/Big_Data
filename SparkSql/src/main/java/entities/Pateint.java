package entities;

import java.util.Date;

public class Pateint {
    private int id_patient;
    private String nom;
    private String prenom;
    private String Cin;
    private String email;
    private String telephone;
    private Date dateNaissance;

    public Pateint(int id_patient, String nom, String prenom, String cin, String email, String telephone, Date dateNaissance) {
        this.id_patient = id_patient;
        this.nom = nom;
        this.prenom = prenom;
        Cin = cin;
        this.email = email;
        this.telephone = telephone;
        this.dateNaissance = dateNaissance;
    }

    public int getId_patient() {
        return id_patient;
    }

    public void setId_patient(int id_patient) {
        this.id_patient = id_patient;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getPrenom() {
        return prenom;
    }

    public void setPrenom(String prenom) {
        this.prenom = prenom;
    }

    public String getCin() {
        return Cin;
    }

    public void setCin(String cin) {
        Cin = cin;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public Date getDateNaissance() {
        return dateNaissance;
    }

    public void setDateNaissance(Date dateNaissance) {
        this.dateNaissance = dateNaissance;
    }
}
