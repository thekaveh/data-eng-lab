from pathlib import Path

from docslib.model import NavItem, Page, Section, SiteModel


def test_section_defaults():
    s = Section(number="2.1", title="Input", level=2, anchor="input")
    assert s.number == "2.1" and s.title == "Input"


def test_page_roundtrip():
    p = Page(src=Path("scenarios/foo.md"), title="foo", number="5.3",
             body="# foo\n", sections=[Section("1", "Purpose", 2, "purpose")],
             see_also=[])
    assert p.title == "foo" and p.sections[0].anchor == "purpose"


def test_navitem_children_default_empty():
    n = NavItem(number="5", title="Scenarios", page=None)
    assert n.children == []


def test_sitemodel_holds_collections():
    sm = SiteModel(pages={}, nav=[], scenarios=[], apps=[])
    assert sm.pages == {} and sm.scenarios == []
