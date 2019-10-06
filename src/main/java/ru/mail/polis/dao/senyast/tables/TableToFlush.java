package ru.mail.polis.dao.senyast.tables;

public class TableToFlush {

    private final Table table;
    private final int generation;
    private final boolean isPoisonPeel;

    public TableToFlush(Table table, int generation, boolean isPoisonPeel) {
        this.table = table;
        this.generation = generation;
        this.isPoisonPeel = isPoisonPeel;
    }

    public TableToFlush(Table table, int generation) {
        this(table, generation, false);
    }

    public Table getTable() {
        return table;
    }

    public int getGeneration() {
        return generation;
    }

    public boolean isPoisonPeel() {
        return isPoisonPeel;
    }
}
