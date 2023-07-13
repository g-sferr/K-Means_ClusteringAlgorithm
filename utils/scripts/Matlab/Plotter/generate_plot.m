% ************************************ DESCRIPTION ************************************
% Script created to visualize the various clusters obtained from the point dataset.
% Main objective is to evaluate how the new centroids shifted
% compared to those that were used to initialize the K-Means algorithm.

% N.B. Check that the contents of the txt file are of the right format,
% i.e. "coordinateValue,coordinateValue,coordinateValue."
% ************************************ DESCRIPTION ************************************


% " generate_plot " command must be insert in command line on matlab shell.
% Once is run enter the name of the path where the txt files are located, "data" in this case.
path = input('Insert the test to plot folder: ', 's');

% File TXT di input
basePath = './';
txtFiles = {strcat(basePath, path, '/dataset_test_1k_C1.txt'), strcat(basePath, path, '/1k_initial_centroids.txt'), strcat(basePath, path, '/1k_final_centroids.txt')};

% Loads the data from the 3 TXT files and inserts them into the array data
data = cell(1, 3);
for i = 1:3
    data{i} = readmatrix(txtFiles ...
        {i});
end

% Figure creation
figure

% Data tracking
plot3(data{1}(:,1), data{1}(:,2), data{1}(:,3), 'bo','Linestyle','None')
hold on
plot3(data{2}(:,1), data{2}(:,2), data{2}(:,3), 'g^','Linestyle','None','MarkerFaceColor', 'g')
plot3(data{3}(:,1), data{3}(:,2), data{3}(:,3), 'rs','Linestyle','None','MarkerFaceColor', 'r')

% Adding labels and title to plot
xlabel('X');
ylabel('Y');
zlabel('Z');
title('Plot 3D dei punti');

% Show grid
grid on;

% View Legend
legend('dataset_test', 'centroids_test', 'results');

% Set the view
view(3);