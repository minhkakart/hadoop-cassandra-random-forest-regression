package com.minhkakart.bigdata.cassandra;


import com.datastax.oss.driver.api.core.cql.Row;

import java.util.UUID;

@SuppressWarnings("unused")
public class PlayerStat {
    private static int count = 0;
    
    private final UUID id;
    private final int age;
    private final int defending;
    private final int dribbling;
    private final int height_cm;
    private final int international_reputation;
    private final int passing;
    private final int physic;
    private final int potential;
    private final int shooting;
    private final int weak_foot;
    private final int weight_kg;
    private final int value_eur;

    public PlayerStat(Row row) {
        count++;
        this.id = row.getUuid("id");
        this.age = row.getInt("age");
        this.defending = row.getInt("defending");
        this.dribbling = row.getInt("dribbling");
        this.height_cm = row.getInt("height_cm");
        this.weight_kg = row.getInt("weight_kg");
        this.international_reputation = row.getInt("international_reputation");
        this.passing = row.getInt("passing");
        this.physic = row.getInt("physic");
        this.potential = row.getInt("potential");
        this.shooting = row.getInt("shooting");
        this.weak_foot = row.getInt("weak_foot");
        this.value_eur = row.getInt("value_eur");
    }

    public UUID getId() {
        return id;
    }

    public int getAge() {
        return age;
    }

    public int getDefending() {
        return defending;
    }

    public int getDribbling() {
        return dribbling;
    }

    public int getHeight_cm() {
        return height_cm;
    }

    public int getInternational_reputation() {
        return international_reputation;
    }

    public int getPassing() {
        return passing;
    }

    public int getPhysic() {
        return physic;
    }

    public int getPotential() {
        return potential;
    }

    public int getShooting() {
        return shooting;
    }

    public int getWeak_foot() {
        return weak_foot;
    }

    public int getWeight_kg() {
        return weight_kg;
    }

    public int getValue_eur() {
        return value_eur;
    }
    
    public static int getCount() {
        return count;
    }
    
    @Override
    public String toString() {
        return count + ". PlayerStat{" +
                "id='" + id.toString() + '\'' +
                ", age=" + age +
                ", defending=" + defending +
                ", dribbling=" + dribbling +
                ", height_cm=" + height_cm +
                ", international_reputation=" + international_reputation +
                ", passing=" + passing +
                ", physic=" + physic +
                ", potential=" + potential +
                ", shooting=" + shooting +
                ", weak_foot=" + weak_foot +
                ", weight_kg=" + weight_kg +
                ", value_eur=" + value_eur +
                '}';
    }
    
    public String toRecord() {
        return id.toString() + "," + age + "," + defending + "," + dribbling + "," + height_cm + "," + international_reputation + "," + passing + "," + physic + "," + potential + "," + shooting + "," + weak_foot + "," + weight_kg + "," + value_eur;
    }
    
    public String toCsv() {
        return age + "," + defending + "," + dribbling + "," + height_cm + "," + international_reputation + "," + passing + "," + physic + "," + potential + "," + shooting + "," + weak_foot + "," + weight_kg + "," + value_eur;
    }
}
