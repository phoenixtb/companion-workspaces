package com.shelfwise.part09.javacaller;

import com.shelfwise.part09.story.FileBackedRepository;

import java.io.IOException;
import java.nio.file.Path;

// The Java pricing team's code.
public final class PricingTeam {
    public static void main(String[] args) {
        FileBackedRepository repo = new FileBackedRepository(Path.of("missing-export.csv"));

        // try { repo.load(); } catch (IOException e) { }
        //   error: exception IOException is never thrown in body of corresponding try statement

        try {
            repo.loadChecked(); // @Throws puts 'throws IOException' in the bytecode, so javac allows the catch
        } catch (IOException e) {
            System.out.println("Java caught " + e.getClass().getSimpleName()); // -> Java caught NoSuchFileException
        }
    }
}
