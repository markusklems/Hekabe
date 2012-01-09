package com.hekabe.dashboard.client.view;

import com.google.gwt.user.client.ui.Composite;
import com.smartgwt.client.widgets.Button;
import com.smartgwt.client.widgets.Canvas;
import com.smartgwt.client.widgets.Label;
import com.smartgwt.client.widgets.form.DynamicForm;
import com.smartgwt.client.widgets.form.fields.ComboBoxItem;
import com.smartgwt.client.widgets.form.fields.FormItem;
import com.smartgwt.client.widgets.form.fields.TextItem;
import com.smartgwt.client.widgets.layout.VLayout;

public class NewClusterCassandraView extends Composite {

	private VLayout layoutCassandra;
	private Label lblNewLabel;
	private DynamicForm dynamicForm_1;
	private TextItem txtClusterName;
	private ComboBoxItem cbCassVersion;
	private ComboBoxItem cbPartitioner;
	private Button btnSwitchToTab3;

	public NewClusterCassandraView() {
		layoutCassandra = new VLayout();
		layoutCassandra.setWidth("750");
		
		lblNewLabel = new Label("Cluster");
		lblNewLabel.setHeight("30");
		layoutCassandra.addMember(lblNewLabel);
		
		dynamicForm_1 = new DynamicForm();
		txtClusterName = new TextItem("newTextItem_1", "Cluster Name");
		cbCassVersion = new ComboBoxItem("cbCassVersion", "Cassandra Version");
		cbCassVersion.setValueMap("1.0");
		cbCassVersion.setDefaultToFirstOption(true);
		cbPartitioner = new ComboBoxItem("newComboBoxItem_3", "Partitioner");
		cbPartitioner.setValueMap("Random Partitioner","Byte Ordered Partitioner");
		cbPartitioner.setDefaultToFirstOption(true);
		
		dynamicForm_1.setFields(new FormItem[] { txtClusterName, cbCassVersion, cbPartitioner });
		layoutCassandra.addMember(dynamicForm_1);
		
		btnSwitchToTab3 = new Button("Next");
		
		layoutCassandra.addMember(btnSwitchToTab3);
		
		initWidget(layoutCassandra);
	}

	public Canvas getPane() {
		return layoutCassandra;
	}
}
