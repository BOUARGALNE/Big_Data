package entities;

import java.util.Date;

public class Consultation {
    private int id_consultation;
    private Pateint pateint;
    private  Medecin medecin;
    private Date dateConsultation;

    public Consultation(int id_consultation, Pateint pateint, Medecin medecin, Date dateConsultation) {
        this.id_consultation = id_consultation;
        this.pateint = pateint;
        this.medecin = medecin;
        this.dateConsultation = dateConsultation;
    }

    public int getId_consultation() {
        return id_consultation;
    }

    public void setId_consultation(int id_consultation) {
        this.id_consultation = id_consultation;
    }

    public Pateint getPateint() {
        return pateint;
    }

    public void setPateint(Pateint pateint) {
        this.pateint = pateint;
    }

    public Medecin getMedecin() {
        return medecin;
    }

    public void setMedecin(Medecin medecin) {
        this.medecin = medecin;
    }

    public Date getDateConsultation() {
        return dateConsultation;
    }

    public void setDateConsultation(Date dateConsultation) {
        this.dateConsultation = dateConsultation;
    }
}
