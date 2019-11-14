package com.amazonaws.hoan.git;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.RawTextComparator;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.filter.PathSuffixFilter;

public class GitConnector implements AutoCloseable {

    private final String url;
    private int numberOfCommits = -1, numberOfCodeCommits = -1;
    private Git git;
    private Repository repository;
    private RevWalk revwalk;

    public GitConnector(final String url) {
        this.url = url;
    }

    public Git getGit() {
        return git;
    }

    public Repository getRepository() {
        return repository;
    }

    public int getNumberOfCommits() {
        return numberOfCommits;
    }

    public int getNumberOfCodeCommits() {
        return numberOfCodeCommits;
    }

    public boolean connect() {
        final File file = new File(url, ".git");
        if (!file.exists()) {
            return false;
        }
        final FileRepositoryBuilder builder = new FileRepositoryBuilder();
        try {
            repository = builder.setGitDir(file).readEnvironment() // scan
                    // environment
                    // GIT_*
                    // variables
                    .findGitDir() // scan up the file system tree
                    .build();
        } catch (final IOException e) {
            e.printStackTrace();
            return false;
        }
        try {
            if (repository.getBranch() == null) {
                return false;
            }
        } catch (final IOException e) {
            return false;
        }
        git = new Git(repository);
        revwalk = new RevWalk(this.repository);
        return true;
    }

    @Override
    public void close() {
        if (this.revwalk != null) {
            this.revwalk.close();
        }
        if (this.git != null) {
            this.git.close();
        }
        if (this.repository != null) {
            this.repository.close();
        }
    }

    public Iterable<RevCommit> log() {
        try {
            return git.log().call();
        } catch (final GitAPIException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Map<String, List<CodeHunk>> getChangeHunks(final String baseCommitId, final String headCommitId,
            final String extension)
            throws RevisionSyntaxException, IOException {
        final RevCommit baseCommit = getCommit(baseCommitId);
        final RevCommit headCommit = getCommit(headCommitId);
        try (OutputStream outputStream = new ByteArrayOutputStream();
                final DiffFormatter df = new DiffFormatter(outputStream)) {
            df.setRepository(repository);
            df.setDiffComparator(RawTextComparator.DEFAULT);
            df.setDetectRenames(true);
            if (extension != null) {
                df.setPathFilter(PathSuffixFilter.create(extension));
            }
            final List<DiffEntry> diffs = df.scan(baseCommit.getTree(), headCommit.getTree());
            final Map<String, List<CodeHunk>> fileCodeHunks = new HashMap<>();
            int end = 0;
            for (final DiffEntry diff : diffs) {
                final String path = diff.getNewPath();
                if ("/dev/null".equals(path)) {
                    continue;
                }
                
                df.format(diff);
                df.flush();
                final String format = outputStream.toString();
                final String hunk = format.substring(end);
                final String content = getFileContent(diff.getNewId().toObjectId());
                final List<CodeHunk> hunks = CodeHunk.parse(hunk, content);
                fileCodeHunks.put(path, hunks);
                end = format.length();
                
            }
            return fileCodeHunks;
        }
    }

    public String getFileContent(final ObjectId objectId, final int objectType) {
        String content = null;
        try {
            final ObjectLoader ldr = repository.open(objectId, objectType);
            content = new String(ldr.getCachedBytes());
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    public String getFileContent(final ObjectId objectId) {
        return getFileContent(objectId, Constants.OBJ_BLOB);
    }

    public RevCommit getCommit(final String commitId) throws RevisionSyntaxException, IOException {
        final ObjectId id = repository.resolve(commitId);
        revwalk.reset();
        final RevCommit commit = revwalk.parseCommit(id);
        revwalk.dispose();
        return commit;
    }

    public RevCommit getHeadCommit() {
        try {
            revwalk.reset();
            return revwalk.parseCommit(repository.resolve(Constants.HEAD));
        } catch (final Exception e) {
        } finally {
            revwalk.dispose();
        }
        return null;
    }

    public String getHeadCommitId() {
        return getHeadCommit().getName();
    }
}
