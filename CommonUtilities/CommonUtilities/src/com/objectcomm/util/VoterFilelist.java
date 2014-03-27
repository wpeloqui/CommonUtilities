package com.objectcomm.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;

public class VoterFilelist {

	public ArrayList<String> execute(String pathname) {

		final ArrayList<String> fileList = new ArrayList<String>();

		Path p = Paths.get(pathname);

		SimpleFileVisitor<Path> fv = new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
					throws IOException {

				//TODO: Check filename to only allow .txt files
				//
				//

				if (file.toString().endsWith(".txt")) {
					fileList.add(file.toString());
				}
				else
				{
					System.out.printf("IGNORED: %s is an invalid filename\n", file);
				}
				return FileVisitResult.CONTINUE;
			}
		};

		try {
			Files.walkFileTree(p, fv);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return fileList;
	}
}