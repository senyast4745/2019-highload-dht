package ru.mail.polis.dao.senyast.model;



import ru.mail.polis.dao.senyast.LSMDao;

import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Generation {

    private Generation(){}

    public static int fromPath(final Path path){
        return fromFileName(path.getFileName().toString());
    }

    /**
     * Get generation FileTable from filename.
     *
     * @param fileName name of FileTable file
     * @return generation number
     */
    private static int fromFileName(final String fileName){
        final String pattern = LSMDao.PREFIX_FILE + "(\\d+)" + LSMDao.SUFFIX_DAT;
        final Pattern regex = Pattern.compile(pattern);
        final Matcher matcher = regex.matcher(fileName);
        if (matcher.find()){
            return Integer.parseInt(matcher.group(1));
        }
        return -1;
    }
}