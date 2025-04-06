import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

class DashboardView:
    """View for the dashboard page of the application"""
    
    def render_dashboard(self, project_name: str, watches: List[Dict[str, Any]], 
                         failure_stats: Optional[Dict[str, Any]] = None):
        """Render the main dashboard"""
        st.header(f"Dashboard: {project_name}")
        
        # Display quick stats at the top
        self.render_watch_stats(watches, failure_stats)
        
        # Display active watches table
        self.render_watch_table(watches)
        
        # Display charts if we have watches
        if watches:
            self.render_charts(watches)
    
    def render_watch_stats(self, watches: List[Dict[str, Any]], 
                          failure_stats: Optional[Dict[str, Any]] = None):
        """Render statistics about watches"""
        # Calculate stats if not provided
        if not failure_stats and watches:
            failure_stats = self._calculate_failure_stats(watches)
            
        # Create three columns for metrics
        col1, col2, col3, col4 = st.columns(4)
        
        # Total watches
        with col1:
            st.metric("Total Watches", len(watches))
        
        # Active watches
        active_watches = sum(1 for w in watches if w.get('is_active', False))
        with col2:
            st.metric("Active Watches", active_watches)
        
        # Battery alerts (watches with battery < 20%)
        if failure_stats:
            with col3:
                st.metric(
                    "Battery Alerts", 
                    failure_stats.get('battery_alerts', 0), 
                    delta=failure_stats.get('battery_alerts_delta', 0),
                    delta_color="inverse"
                )
            
            # Sync failures
            with col4:
                st.metric(
                    "Sync Failures", 
                    failure_stats.get('sync_failures', 0),
                    delta=failure_stats.get('sync_failures_delta', 0),
                    delta_color="inverse"
                )
    
    def render_watch_table(self, watches: List[Dict[str, Any]]):
        """Render a table of watches"""
        if not watches:
            st.info("No watches found for this project.")
            return
            
        st.subheader("Watch Status")
        
        # Convert to DataFrame for display
        watch_df = pd.DataFrame(watches)
        
        # Format the DataFrame
        if 'battery_level' in watch_df.columns:
            watch_df['battery_level'] = watch_df['battery_level'].apply(
                lambda x: f"{x}%" if x is not None else "Unknown"
            )
        
        if 'is_active' in watch_df.columns:
            watch_df['status'] = watch_df['is_active'].apply(
                lambda x: "Active" if x else "Inactive"
            )
            watch_df = watch_df.drop(columns=['is_active'])
        
        # Format timestamps
        if 'last_sync_time' in watch_df.columns:
            watch_df['last_sync_time'] = watch_df['last_sync_time'].apply(
                lambda x: datetime.fromisoformat(x).strftime("%Y-%m-%d %H:%M") if x else "Never"
            )
        
        # Display the table
        st.dataframe(
            watch_df,
            column_config={
                "name": "Watch Name",
                "battery_level": "Battery",
                "last_sync_time": "Last Sync",
                "assigned_to": "Assigned To",
                "status": "Status"
            },
            use_container_width=True
        )
    
    def render_charts(self, watches: List[Dict[str, Any]]):
        """Render charts for watch data"""
        st.subheader("Data Visualization")
        
        # Choose what to show based on available data
        tab1, tab2 = st.tabs(["Battery Levels", "Sync Status"])
        
        with tab1:
            self._render_battery_chart(watches)
        
        with tab2:
            self._render_sync_chart(watches)
    
    def _render_battery_chart(self, watches: List[Dict[str, Any]]):
        """Render a chart of battery levels"""
        # Filter watches with battery data
        battery_data = [w for w in watches if w.get('battery_level') is not None]
        
        if not battery_data:
            st.info("No battery data available")
            return
        
        # Create a DataFrame for the chart
        battery_df = pd.DataFrame([
            {
                'name': w['name'],
                'battery': int(str(w['battery_level']).replace('%', '')) if isinstance(w['battery_level'], (int, str)) else 0
            }
            for w in battery_data
        ])
        
        # Create color mapping based on battery level
        battery_df['color'] = battery_df['battery'].apply(
            lambda x: 'red' if x < 20 else ('yellow' if x < 50 else 'green')
        )
        
        # Sort by battery level
        battery_df = battery_df.sort_values('battery')
        
        # Create the chart
        fig = px.bar(
            battery_df,
            x='name',
            y='battery',
            color='color',
            color_discrete_map={'red': 'red', 'yellow': '#FFA500', 'green': 'green'},
            labels={'name': 'Watch Name', 'battery': 'Battery Level (%)'},
            title='Watch Battery Levels'
        )
        
        # Update layout
        fig.update_layout(
            showlegend=False,
            xaxis={'categoryorder': 'total descending'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_sync_chart(self, watches: List[Dict[str, Any]]):
        """Render a chart of sync times"""
        # Filter watches with sync data
        sync_data = [w for w in watches if w.get('last_sync_time')]
        
        if not sync_data:
            st.info("No sync data available")
            return
        
        # Create a DataFrame for the chart
        sync_df = pd.DataFrame([
            {
                'name': w['name'],
                'last_sync': datetime.fromisoformat(w['last_sync_time']) if w.get('last_sync_time') else None
            }
            for w in sync_data
        ])
        
        # Calculate hours since last sync
        now = datetime.now()
        sync_df['hours_since_sync'] = sync_df['last_sync'].apply(
            lambda x: (now - x).total_seconds() / 3600 if x else None
        )
        
        # Create color mapping based on hours since sync
        sync_df['color'] = sync_df['hours_since_sync'].apply(
            lambda x: 'red' if x > 24 else ('yellow' if x > 12 else 'green')
        )
        
        # Sort by hours since sync
        sync_df = sync_df.sort_values('hours_since_sync')
        
        # Create the chart
        fig = px.bar(
            sync_df,
            x='name',
            y='hours_since_sync',
            color='color',
            color_discrete_map={'red': 'red', 'yellow': '#FFA500', 'green': 'green'},
            labels={'name': 'Watch Name', 'hours_since_sync': 'Hours Since Last Sync'},
            title='Watch Sync Status'
        )
        
        # Update layout
        fig.update_layout(
            showlegend=False,
            xaxis={'categoryorder': 'total descending'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _calculate_failure_stats(self, watches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate failure statistics from watch data"""
        # Count battery alerts
        battery_alerts = sum(
            1 for w in watches 
            if isinstance(w.get('battery_level'), (int, str)) and 
            int(str(w['battery_level']).replace('%', '')) < 20
        )
        
        # Count sync failures (no sync in last 24 hours)
        now = datetime.now()
        sync_failures = sum(
            1 for w in watches
            if w.get('last_sync_time') and 
            (now - datetime.fromisoformat(w['last_sync_time'])).total_seconds() > 24 * 3600
        )
        
        return {
            'battery_alerts': battery_alerts,
            'sync_failures': sync_failures
        }
