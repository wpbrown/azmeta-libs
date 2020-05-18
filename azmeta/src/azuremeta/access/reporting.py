from nbconvert import HTMLExporter
from nbconvert.preprocessors import Preprocessor, TagRemovePreprocessor
from nbformat import NotebookNode
from traitlets.config import Config
from traitlets import Bool


class CustomCssPreprocessor(Preprocessor):

    full_width = Bool(False)
    full_width.tag(config=True)

    def preprocess(self, nb, resources):
        custom_css = '''
        body { width: max-content; }
        .container { width: unset; }
        div.output_subarea { overflow-x: visible; max-width: 100%; }
        ''' if self.full_width else '''
        .container { width: 95%; }
        div.output_subarea { max-width: 100%; }
        ''' 
        resources['inlining']['css'].append(custom_css)
        return nb, resources


def export_nodebook_node_to_html(node: NotebookNode, output_path: str, full_width:bool = False) -> None:
    normal_config = Config()
    normal_config.CustomCssPreprocessor.full_width = full_width
    normal_config.HTMLExporter.preprocessors = [CustomCssPreprocessor]
    html_exporter = HTMLExporter(config=normal_config)
    report_config = Config()
    report_config.CustomCssPreprocessor.full_width = full_width
    report_config.TemplateExporter.exclude_input = True
    report_config.TemplateExporter.exclude_input_prompt = True
    report_config.TemplateExporter.exclude_output_prompt = True
    report_config.TagRemovePreprocessor.remove_cell_tags = ("report_exclude",)
    report_config.HTMLExporter.preprocessors = [TagRemovePreprocessor, CustomCssPreprocessor]
    html_exporter_report = HTMLExporter(config=report_config)

    # content, _ = html_exporter.from_notebook_node(node)
    # with open(f'{output_path}.src.html', 'w') as html_file:
    #     html_file.write(content)
    content, _ = html_exporter_report.from_notebook_node(node)
    with open(output_path, 'w') as html_file:
        html_file.write(content)
