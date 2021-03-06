package com.hekabe.dashboard.client.view;

import com.hekabe.dashboard.client.CommunicationServiceAsync;
import com.hekabe.dashboard.client.Dashboard;
import com.smartgwt.client.types.Overflow;
import com.smartgwt.client.widgets.layout.VLayout;

public class MgmtView extends VLayout {

	private MgmtClustersView clusterView;
	private MgmtDetailView detailView;
	private Dashboard dashboard;
	@SuppressWarnings("unused")
	private CommunicationServiceAsync rpcService;
	
	/**
	 * Creates ManagementView
	 * @param dashboard
	 * @param rpcService
	 */
	public MgmtView(Dashboard dashboard, CommunicationServiceAsync rpcService) {
		this.dashboard = dashboard;
		this.rpcService = rpcService;
		
		clusterView = new MgmtClustersView(this, rpcService);
		detailView = new MgmtDetailView(this, rpcService);
		
		clusterView.setMargin(10);
		detailView.setMargin(10);
		
		addMember(clusterView);
		addMember(detailView);
		setOverflow(Overflow.VISIBLE);
		setAutoHeight();
	}

	/**
	 * 
	 * @return
	 */
	public MgmtClustersView getClusterView() {
		return clusterView;
	}

	/**
	 * 
	 * @return
	 */
	public MgmtDetailView getDetailView() {
		return detailView;
	}
	
	/**
	 * 
	 * @return
	 */
	public Dashboard getDashboard() {
		return dashboard;
	}
}