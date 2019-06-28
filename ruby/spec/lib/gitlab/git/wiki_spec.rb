require 'spec_helper'

describe Gitlab::Git::Wiki do
  include TestRepo

  let(:repository) { gitlab_git_from_gitaly(new_empty_test_repo) }

  subject { described_class.new(repository) }

  describe '#pages' do
    let(:pages) { subject.pages }

    before do
      create_page('page1', 'content')
      create_page('page2', 'content2')
    end

    after do
      destroy_page('page1')
      destroy_page('page2')
    end

    it 'returns all the pages' do
      expect(pages.count).to eq(2)
      expect(pages.first.title).to eq 'page1'
      expect(pages.last.title).to eq 'page2'
    end

    it 'returns only one page' do
      pages = subject.pages(limit: 1)

      expect(pages.count).to eq(1)
      expect(pages.first.title).to eq 'page1'
    end

    it 'returns formatted data' do
      expect(pages.first.formatted_data).to be_a(String)
    end
  end

  describe '#page' do
    before do
      create_page('page1', 'content')
      create_page('foo/page1', 'content foo/page1')
    end

    after do
      destroy_page('page1')
      destroy_page('page1', 'foo')
    end

    it 'returns the right page' do
      expect(subject.page(title: 'page1', dir: '').url_path).to eq 'page1'
      expect(subject.page(title: 'page1', dir: 'foo').url_path).to eq 'foo/page1'
    end

    it 'returns formatted data' do
      expect(subject.page(title: 'page1', dir: '').formatted_data).to be_a(String)
    end
  end

  describe '#delete_page' do
    after do
      destroy_page('page1')
    end

    it 'only removes the page with the same path' do
      create_page('page1', 'content')
      create_page('*', 'content')

      subject.delete_page('*', commit_details('whatever'))

      expect(subject.pages.count).to eq 1
      expect(subject.pages.first.title).to eq 'page1'
    end
  end

  describe '#update_page' do
    let(:old_title) { 'page1' }
    let(:new_content) { 'different content' }
    let(:new_title) { 'new title' }
    let(:deets) { commit_details('update') }

    before do
      create_page(old_title, 'some content')
    end
    after do
      destroy_page(new_title)
    rescue Gitlab::Git::Wiki::PageNotFound
      destroy_page(old_title)
    end

    it 'can update the page' do
      subject.update_page(old_title, new_title, :markdown, new_content, deets)

      expect(subject.pages.count).to eq 1
      expect(subject.pages.first.title).to eq new_title
      expect(subject.pages.first.text_data).to eq new_content
    end

    it 'raises PageNotFound when trying to access an unknown page' do
      expect { subject.update_page('bad path', new_title, :markdown, new_content, deets) }
        .to raise_error(Gitlab::Git::Wiki::PageNotFound)
    end
  end

  def create_page(name, content)
    subject.write_page(name, :markdown, content, commit_details(name))
  end

  def commit_details(name)
    Gitlab::Git::Wiki::CommitDetails.new(1, 'test-user', 'Test User', 'test@example.com', "created page #{name}")
  end

  def destroy_page(title, dir = '')
    page = subject.page(title: title, dir: dir)

    raise Gitlab::Git::Wiki::PageNotFound, title unless page

    subject.delete_page(page.path, commit_details(title))
  end
end
